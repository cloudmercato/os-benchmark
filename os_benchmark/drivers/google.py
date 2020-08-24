"""
.. note::
  This driver requires `google-cloud-storage`_.

`Google Storage`_ from `Google Cloud`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  google:
    driver: google
    location: eu-central1
    json: <Service JSON>

.. _oci: https://pypi.org/project/google-cloud-storage/
.. _`Google Storage`: https://cloud.google.com/storage/
.. _`Google Cloud`: https://cloud.google.com/
"""
import concurrent.futures
from google.cloud import storage
from google.cloud.client import service_account
from google.api_core import exceptions
from google.resumable_media import InvalidResponse
from os_benchmark.drivers import base, errors

BACLS = {
    'public-read': 'public-read',
}
OACLS = {
    'public-read': 'publicread',
}


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 'google'

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self.credentials = service_account.Credentials.from_service_account_info(
                self.kwargs['json'],
            )
            self._client = storage.Client(
                credentials=self.credentials,
                project=self.kwargs['json']['project_id'],
                # _http=self.session,
            )
        return self._client

    def list_buckets(self, **kwargs):
        buckets = self.client.list_buckets()
        return [{'id': c.name} for c in buckets]

    def create_bucket(self, name, acl='public-read', **kwargs):
        acl = BACLS.get(acl)
        params = {
            'bucket_or_name': name,
            'location': self.kwargs.get('location'),
            'predefined_acl': acl,
        }
        try:
            bucket = self.client.create_bucket(**params)
        except exceptions.Conflict as err:
            raise errors.DriverBucketAlreadyExistError(err)
        return {
            'id': bucket.name,
            'name': name,
        }

    def delete_bucket(self, bucket_id, **kwargs):
        bucket = storage.Bucket(self.client, bucket_id)
        try:
            bucket.delete()
        except exceptions.NotFound:
            return

    def list_objects(self, bucket_id, **kwargs):
        bucket = storage.Bucket(self.client, bucket_id)
        objs = bucket.list_blobs()
        return [o.name for o in objs]

    def _simple_upload(self, bucket_id, name, **params):
        bucket = storage.Bucket(self.client, bucket_id)
        blob = storage.Blob(name=name, bucket=bucket)
        try:
            blob._do_multipart_upload(**params)
        except InvalidResponse as err:
            raise

    def _multipart_upload(self, bucket_id, name, multipart_chunksize, max_concurrency=None,
                          **params):
        multipart_chunksize = multipart_chunksize or base.MULTIPART_CHUNKSIZE
        max_concurrency = max_concurrency or base.MAX_CONCURRENCY

        part_id = 1
        offset = 0
        parts = []

        def _upload(part_id, offset):
            self.logger.debug('Uploading %s part %s', name, part_id)
            part_name = '%s-%s' % (name, part_id)
            part = base.MultiPart(params['stream'], multipart_chunksize)
            part_params = params.copy()
            part_params.update({
                'stream': part,
                'size': part.size,
            })
            self._simple_upload(bucket_id, part_name, **part_params)
            parts.append(part_name)
            self.logger.debug('Done %s part %s', name, part_id)

        pool_kwargs = {'max_workers': max_concurrency}
        with concurrent.futures.ThreadPoolExecutor(**pool_kwargs) as executor:
            futures = []
            while offset < params['size']:
                chunk_size = min(multipart_chunksize, params['size'] - offset)
                result = executor.submit(_upload, part_id=part_id, offset=offset)
                futures.append(result)
                part_id += 1
                offset += multipart_chunksize
            for future in futures:
                future.result()

        def compose_objects(params, filename):
            url = 'https://storage.googleapis.com/storage/v1/b/%s/o/%s/compose' % (
                bucket_id, filename
            )
            response = self.client._http.post(url, json=params)
            self.logger.info("Composing %s: %s", filename, params)
            if response.status_code != 200:
                raise Exception(response.content)
            self.logger.info("Composed %s" % filename)

        to_delete = []
        compose_params = {
            "destination": {
                "contentType": params['content_type']
            }
        }
        if len(parts) <= 32:
            parts = sorted(parts, key=lambda n: int(n.split('-')[-1]))
            compose_params["sourceObjects"] = [
                {'name': part} for part in parts
            ]
            compose_objects(compose_params, name)
            to_delete.extend(parts)
        else:
            while parts:
                parts = sorted(parts, key=lambda n: int(n.split('-')[-1]))
                to_compose = parts[:32]
                del parts[:32]
                compose_params["sourceObjects"] = [
                    {'name': part} for part in to_compose
                ]
                compose_name = name
                compose_objects(compose_params, compose_name)
                to_delete.extend(to_compose)

        bucket = storage.Bucket(self.client, bucket_id)
        bucket.delete_blobs(to_delete)

    def upload(self, bucket_id, name, content, max_concurrency=None,
               multipart_chunksize=None, multipart_threshold=None,
               validate_content=False, acl='public-read', **kwargs):
        multipart_threshold = multipart_threshold or base.MULTIPART_THRESHOLD
        acl = OACLS[acl]

        params = {
            'stream': content,
            'client': self.client,
            'content_type': 'application/octet-stream',
            'size': content.size,
            'timeout': (self.connect_timeout, self.read_timeout),
            'num_retries': 0,
            'predefined_acl': acl,
            'if_generation_match': None,
            'if_generation_not_match': None,
            'if_metageneration_match': None,
            'if_metageneration_not_match': None,
        }
        if content.size >= multipart_threshold:
            self._multipart_upload(
                bucket_id=bucket_id,
                name=name,
                multipart_chunksize=multipart_chunksize,
                max_concurrency=max_concurrency,
                **params
            )
        else:
            self._simple_upload(
                bucket_id=bucket_id,
                name=name,
                **params
            )
        return {'name': name}

    def delete_object(self, bucket_id, name, **kwargs):
        bucket = storage.Bucket(self.client, bucket_id)
        bucket.delete_blob(blob_name=name)

    def get_url(self, bucket_id, name, **kwargs):
        bucket = storage.Bucket(self.client, bucket_id)
        blob = storage.Blob(name=name, bucket=bucket)
        url = blob._get_download_url(self.client)
        return url
