"""
.. note::
  This driver requires `azure-storage-blob`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  azure:
    driver: azure
    connect_str: <connect_str>

.. _azure-storage-blob: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
"""
from azure.storage import blob
from os_benchmark.drivers import base, errors

ACLS = {
    'public-read': 'container',
}


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 'azure'
    client_kwargs = {}

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = blob.BlobServiceClient.from_connection_string(
                self.kwargs['connect_str'],
                **self.client_kwargs,
            )
        return self._client

    def setup(self, **kwargs):
        if 'multipart_chunksize' in kwargs:
            self.client_kwargs.update({
                'max_block_size': kwargs['multipart_chunksize'],
                'max_page_size': kwargs['multipart_chunksize'],
            })
        if 'multipart_threshold' in kwargs:
            self.client_kwargs['max_single_put_size'] = kwargs['multipart_threshold']
        self.logger.debug("Azure default config: %s", self.client_kwargs)
        self.client  # Avoid lazyness

    def list_buckets(self, **kwargs):
        containers = self.client.list_containers()
        return [{'id': c.name} for c in containers]

    def create_bucket(self, name, acl='public-read', **kwargs):
        bucket = self.client.create_container(
            name=name,
            public_access=ACLS.get(acl, None),
        )
        return {'id': name}

    def delete_bucket(self, bucket_id, **kwargs):
        try:
            self.client.delete_container(bucket_id)
        except blob.ResourceNotFoundError:
            pass

    def list_objects(self, bucket_id, **kwargs):
        client = self.client.get_container_client(bucket_id)
        blobs = client.list_blobs()
        return [b.name for b in blobs]

    def upload(self, bucket_id, name, content, max_concurrency=None,
               validate_content=False, **kwargs):
        max_concurrency = max_concurrency or base.MAX_CONCURRENCY
        client = self.client.get_container_client(bucket_id)
        client.upload_blob(
            name=name,
            data=content,
            max_concurrency=max_concurrency,
            timeout=self.read_timeout,
            validate_content=validate_content,
        )
        return {'name': name}

    def delete_object(self, bucket_id, name, **kwargs):
        client = self.client.get_container_client(bucket_id)
        client.delete_blob(name)

    def get_url(self, bucket_id, name, **kwargs):
        url = 'https://%s.blob.core.windows.net/%s/%s' % (
            self.client.account_name, bucket_id, name,
        )
        return url
