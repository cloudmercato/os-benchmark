"""
.. note::
  This driver requires `oss2`_.

`Object Storage Service`_ from `Alibaba Cloud`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  alibaba:
    driver: alibaba
    access_key_id: <your_ak>
    access_key_secret: <your_sk>
    endpoint: https://oss-<region_id>.aliyuncs.com

.. _oss2: https://github.com/aliyun/aliyun-oss-python-sdk
.. _`Object Storage Service`: https://www.alibabacloud.com/product/oss
.. _`Alibaba Cloud`: https://www.alibabacloud.com/
"""
import concurrent.futures
from functools import wraps
from urllib.parse import urlparse
import oss2
from os_benchmark.drivers import base, errors


class AliSession(oss2.Session):
    def __init__(self, raw_session):
        self.session = raw_session


def handle_request(method):
    @wraps(method)
    def _handle_request(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except oss2.exceptions.ServerError as err:
            if err.status == 403:
                self.logger.warning("Did you activate OSS in the web console?")
                raise errors.DriverPermissionError(err)
            raise
        except oss2.exceptions.RequestError as err:
            if err.status == -2:
                raise errors.DriverConnectionError(err.exception.args[0].reason.args[0])
            raise
    return _handle_request


class Driver(base.RequestsMixin, base.BaseDriver):
    """Alibaba OSS Driver"""
    id = 'alibaba'

    @property
    def auth(self):
        if not hasattr(self, '_auth'):
            self._auth = oss2.Auth(self.kwargs['access_key_id'], self.kwargs['access_key_secret'])
        return self._auth

    @property
    def service(self):
        if not hasattr(self, '_service'):
            self._service = oss2.Service(self.auth, self.kwargs['endpoint'])
        return self._service

    @property
    def ali_session(self):
        if not hasattr(self, '_ali_session'):
            self._ali_session = AliSession(self.session)
        return self._ali_session

    def _get_bucket(self, name):
        bucket = oss2.Bucket(
            auth=self.auth,
            endpoint=self.kwargs['endpoint'],
            bucket_name=name,
            session=self.ali_session,
            connect_timeout=self.connect_timeout,
        )
        return bucket

    @handle_request
    def list_buckets(self, **kwargs):
        raw_buckets = oss2.BucketIterator(self.service)
        buckets = [{'id': b.name} for b in raw_buckets]
        return buckets

    @handle_request
    def create_bucket(self, name, acl='public-read', **kwargs):
        bucket = self._get_bucket(name)
        bucket.create_bucket(
            permission=acl,
            input=oss2.models.BucketCreateConfig('Standard'),
        )
        return {'id': name}

    @handle_request
    def clean_multipart(self, bucket_id, **kwargs):
        bucket = self._get_bucket(bucket_id)
        parts = bucket.list_multipart_uploads().upload_list
        for part in parts:
            self.logger.debug('Aborting multipart %s/%s %s', bucket_id, part.key, part.upload_id)
            bucket.abort_multipart_upload(
                key=part.key,
                upload_id=part.upload_id,
            )

    @handle_request
    def delete_bucket(self, bucket_id, clean_multipart=True, **kwargs):
        bucket = self._get_bucket(bucket_id)
        if clean_multipart:
            self.clean_multipart(bucket_id)

        try:
            bucket.delete_bucket()
        except oss2.exceptions.BucketNotEmpty as err:
            raise errors.DriverNonEmptyBucketError(err)

    @handle_request
    def list_objects(self, bucket_id, **kwargs):
        bucket = self._get_bucket(bucket_id)
        raw_objects = bucket.list_objects().object_list
        objects = [o.key for o in raw_objects]
        return objects

    def _simple_upload(self, bucket_id, name, content, acl='public-read'):
        bucket = self._get_bucket(bucket_id)
        bucket.put_object(name, content)

    def _multipart_upload(self, bucket_id, name, content, acl='public-read', multipart_chunksize=None, max_concurrency=None):
        bucket = self._get_bucket(bucket_id)
        multipart_chunksize = multipart_chunksize or base.MULTIPART_CHUNKSIZE
        max_concurrency = max_concurrency or base.MAX_CONCURRENCY

        content_size = content.size
        part_id = 1
        offset = 0
        parts = []
        upload_id = bucket.init_multipart_upload(name).upload_id

        def _upload(part_id, offset):
            self.logger.debug('Uploading %s part %s', name, part_id)
            result = bucket.upload_part(name, upload_id, part_id, oss2.SizedFileAdapter(content, multipart_chunksize))
            self.logger.debug('Done %s part %s', name, part_id)
            parts.append(oss2.models.PartInfo(part_id, result.etag, size=multipart_chunksize, part_crc=result.crc))

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

        bucket.complete_multipart_upload(name, upload_id, parts)

    @handle_request
    def upload(self, bucket_id, name, content, acl='public-read',
               multipart_threshold=None, multipart_chunksize=None,
               max_concurrency=None,
               **kwargs):
        multipart_threshold = multipart_threshold or base.MULTIPART_THRESHOLD
        if content.size > multipart_threshold:
            self._multipart_upload(bucket_id, name, content, acl, multipart_chunksize, max_concurrency)
        else:
            self._simple_upload(bucket_id, name, content, acl)
        return {'name': name}

    @handle_request
    def delete_object(self, bucket_id, name, **kwargs):
        bucket = self._get_bucket(bucket_id)
        bucket.delete_object(name)

    def get_url(self, bucket_id, name, **kwargs):
        bucket = self._get_bucket(bucket_id)
        hostname = urlparse(self.kwargs['endpoint']).netloc
        url = 'https://%s.%s/%s' % (bucket_id, hostname, name)
        return url
