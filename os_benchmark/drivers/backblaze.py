"""
.. note::
  This driver requires `b2sdk`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  backblaze:
    driver: backblaze
    application_key_id: <key_id>
    application_key: <key>

.. _b2sdk: https://github.com/Backblaze/b2-sdk-python
"""
from functools import wraps
import hashlib
from requests.packages.urllib3.util.retry import Retry
from b2sdk import v2 as b2
from b2sdk.v2 import api, exception, AbstractUploadSource
from os_benchmark.drivers import base, errors

ACLS = {
    'public-read': 'allPublic',
    'private': 'allPrivate',
}


class UploadSourceFileIo(AbstractUploadSource):
    def __init__(self, file):
        self.file = file

    def get_content_length(self):
        return self.file.size

    def get_content_sha1(self):
        self.file.seek(0)
        return hashlib.sha1(self.file.read()).hexdigest()

    def open(self):
        return self.file

    def seek(self, offset):
        self.file.seek(offset)

    def read(self, size=None):
        return self.file.read(size)


def handle_request(method):
    @wraps(method)
    def _handle_request(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except exception.B2ConnectionError as err:
            raise errors.DriverConnectionError(err)
        except exception.B2RequestTimeoutDuringUpload as err:
            raise errors.DriverConnectionError(err)
        except exception.TooManyRequests as err:
            raise errors.DriverServerError(err)
    return _handle_request


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 'backblaze'

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._account_info = b2.InMemoryAccountInfo()
            self._client = api.B2Api(self._account_info)
            self.client.authorize_account(
                'production',
                self.kwargs['application_key_id'],
                self.kwargs['application_key']
            )
            retry = Retry(
                total=self.retry,
                connect=self.connect_retry,
                read=self.read_retry,
                status=self.status_retry,
                status_forcelist=self.retry_status_codes,
                backoff_factor=0,
                redirect=0
            )
            timeout = (self.connect_timeout, self.read_timeout)
            adapter = base.HTTPAdapter(max_retries=retry, timeout=timeout)
            self._client.raw_api.b2_http.session.mount('http://', adapter)
            self._client.raw_api.b2_http.session.mount('https://', adapter)
            self._client.raw_api.b2_http.TIMEOUT = timeout
        return self._client

    @handle_request
    def list_buckets(self, **kwargs):
        buckets = self.client.list_buckets()
        return [{'id': c.id_} for c in buckets]

    @handle_request
    def create_bucket(self, name, acl='public-read', **kwargs):
        acl = ACLS.get(acl)
        bucket = self.client.create_bucket(
            name=name,
            bucket_type=acl,
        )
        return {
            'id': bucket.id_,
            'name': name,
        }

    def _get_bucket(self, bucket_id):
        bucket = self.client.get_bucket_by_id(bucket_id)
        return bucket

    def delete_bucket(self, bucket_id, **kwargs):
        bucket = self._get_bucket(bucket_id)
        try:
            unfinisheds = bucket.list_unfinished_large_files()
        except exception.NonExistentBucket:
            return
        for unfinished in unfinisheds:
            bucket.cancel_large_file(unfinished.file_id)

        try:
            self.client.delete_bucket(bucket)
        except exception.BadRequest as err:
            if err.code == 'cannot_delete_non_empty_bucket':
                raise errors.DriverNonEmptyBucketError(err.message)
            raise

    def list_objects(self, bucket_id, **kwargs):
        bucket = self._get_bucket(bucket_id)
        objs = bucket.ls()
        return [o.file_name for o, _ in objs]

    def _simple_upload(self, bucket_id, name, upload_source):
        bucket = self._get_bucket(bucket_id)
        content_length = upload_source.get_content_length()
        bucket.api.session.upload_file(
            bucket_id=bucket_id,
            file_name=name,
            content_length=content_length,
            content_type='application/octet-stream',
            content_sha1='do_not_verify',
            file_infos={},
            data_stream=upload_source,
        )

    def _multipart_upload(self, bucket_id, name, content, multipart_chunksize=None, max_concurrency=None):
        bucket = self._get_bucket(bucket_id)

        def _upload(part_id, offset, content, file_id):
            self.logger.debug('Uploading %s part %s', name, part_id)
            part = base.MultiPart(content, multipart_chunksize)
            upload_source = UploadSourceFileIo(part)
            result = bucket.api.session.upload_part(
                file_id=file_id,
                part_number=part_id,
                sha1_sum='do_not_verify',
                content_length=part.size,
                input_stream=upload_source,
            )
            self.logger.debug('Done %s part %s', name, part_id)
            return result

        result = bucket.api.session.start_large_file(
            bucket_id=bucket_id,
            file_name=name,
            content_type='application/octet-stream',
            file_info={}
        )
        file_id = result['fileId']

        uploader = base.MultiPartUploader(
            content=content,
            multipart_chunksize=multipart_chunksize,
            max_concurrency=max_concurrency,
            extra_upload_kwargs={
                'file_id': file_id,
            }
        )
        parts = uploader.run(_upload)

        bucket.api.session.finish_large_file(
            file_id=file_id,
            part_sha1_array=[
                p['contentSha1'].replace('unverified:', '')
                for p in parts
            ],
        )

    @handle_request
    def upload(self, bucket_id, name, content, max_concurrency=None,
               multipart_chunksize=None, multipart_threshold=None,
               validate_content=False, **kwargs):
        multipart_threshold = multipart_threshold or base.MULTIPART_THRESHOLD
        multipart_chunksize = multipart_chunksize or base.MULTIPART_CHUNKSIZE

        try:
            if content.size > multipart_threshold:
                self._multipart_upload(
                    bucket_id,
                    name,
                    content,
                    multipart_chunksize,
                    max_concurrency
                )
            else:
                upload_source = UploadSourceFileIo(content)
                self._simple_upload(bucket_id, name, upload_source)
        except exception.StorageCapExceeded as err:
            raise errors.DriverStorageQuotaError(err)
        return {'name': name}

    @handle_request
    def delete_object(self, bucket_id, name, **kwargs):
        bucket = self._get_bucket(bucket_id)
        versions = bucket.list_file_versions(name)
        for version in versions:
            try:
                bucket.delete_file_version(
                    version.id_,
                    name,
                )
            except exception.FileNotPresent as err:
                self.logger.debug(err)

    def get_url(self, bucket_id, name, **kwargs):
        bucket = self._get_bucket(bucket_id)
        url = bucket.get_download_url(name)
        return url
