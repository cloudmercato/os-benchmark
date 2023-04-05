"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  gridscale:
    driver: gridscale
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    region: <bucket_region>

You can manually set ``endpoint_url`` or use a region below as shortcut:

- ``de/fra2`` (gos3.io)
- ``ch/app1`` (bc01.gos3.io)
- ``nl/ams1`` (ce21.gos3.io)

See the `official tutorial`_ for more details.

.. _boto3: https://github.com/boto/boto3
.. _official tutorial: https://gridscale.io/en/community/tutorials/quick-guide-s3-compatible-object-storage/
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Gridscale S3 Driver"""
    id = 'gridscale'

    ENDPOINTS = {
        'de/fra2': 'gos3.io',
        'ch/app1': 'bc01.gos3.io',
        'nl/ams1': 'ce21.gos3.io',
    }

    def __init__(self, *args, **kwargs):
        if 'region' in kwargs:
            self.region = kwargs.pop('region')
            endpoint_url = 'https://%s' % self.ENDPOINTS[self.region]
            kwargs.setdefault('endpoint_url', endpoint_url)
        super().__init__(*args, **kwargs)

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (
            self.kwargs['endpoint_url'],
            bucket_id,
            name,
        )
        return url
