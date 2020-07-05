"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  wasabi:
    driver: wasabi
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

Possible region IDs are the following:

- ``us-east-1``: US East 1 (N. Virginia)
- ``us-east-2``: US East 2 (N. Virginia)
- ``us-west-1``: US West 1 (Oregon)
- ``eu-central-1``: EU Central 1 (Amsterdam)

.. _boto3: https://github.com/boto/boto3
"""
from urllib.parse import urljoin
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Wasabi S3 Driver"""
    id = 'wasabi'
    endpoint_url = 'https://s3.wasabisys.com'
    endpoint_urls = {
        'us-east-1': 'https://s3.us-east-1.wasabisys.com',
        'us-east-2': 'https://s3.us-east-2.wasabisys.com',
        'us-west-1': 'https://s3.us-west-1.wasabisys.com',
        'eu-central-1': 'https://s3.eu-central-1.wasabisys.com',
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
        url = urljoin(self.endpoint_url, '%s/%s' % (bucket_id, name))
        return url
