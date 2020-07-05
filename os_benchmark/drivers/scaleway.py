"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  scalewey:
    driver: scaleway
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    region_name: <region_name>

Possible ``<region_name>`` are the following:

- ``fr-par``: Paris, France
- ``nl-ams``: Amsterdam, Netherland

.. _boto3: https://github.com/boto/boto3
"""
from urllib.parse import urljoin
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Scaleway S3 Driver"""
    id = 'scaleway'
    endpoint_url = 'https://s3.fr-par.scw.cloud'
    endpoint_urls = {
        'fr-par': 'https://s3.fr-par.scw.cloud',
        'nl-ams': 'https://s3-nl-ams.scw.cloud',
    }
    default_kwargs = {
        'endpoint_url': endpoint_url,
    }

    def get_custom_kwargs(self, kwargs):
        if 'region_name' in kwargs:
            endpoint_url = self.endpoint_urls.get(kwargs['region_name'])
            self.endpoint_url = endpoint_url
        kwargs['endpoint_url'] = self.endpoint_url
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        url = urljoin(self.endpoint_url, '%s/%s' % (bucket_id, name))
        return url
