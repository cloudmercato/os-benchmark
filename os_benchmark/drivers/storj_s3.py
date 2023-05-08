"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  storj:
    driver: storj
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

Possible region IDs are the following:

- ``EU1``: US East 1 (N. Virginia)

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Storj S3 Driver"""
    id = 'storj'
    endpoint_url = 'https://gateway.storjshare.io'
    endpoint_urls = {
        'eu1': 'https://gateway.eu1.storjshare.io',
        'us1': 'https://gateway.us1.storjshare.io',
        'ap1': 'https://gateway.ap1.storjshare.io',
    }

    default_kwargs = {
        'endpoint_url': endpoint_url,
    }

    def get_custom_kwargs(self, kwargs):
        if 'region_name' in kwargs:
            endpoint_url = self.endpoint_urls.get(kwargs['region_name'])
            kwargs['endpoint_url'] = endpoint_url
            self.endpoint_url = endpoint_url
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        url = self.get_presigned_url(
            bucket_id=bucket_id,
            name=name,
        )
        return url
