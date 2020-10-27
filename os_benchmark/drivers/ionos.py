"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  ionos:
    driver: ionos
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """IONOS S3 Driver"""
    id = 'ionos'
    endpoint_url = 'https://s3-de-central.profitbricks.com'

    def get_custom_kwargs(self, kwargs):
        kwargs['endpoint_url'] = self.endpoint_url
        kwargs['region_name'] = 'de-central'
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.endpoint_url, bucket_id, name)
        return url
