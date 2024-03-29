"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  tebi:
    driver: tebi
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Tebi S3 Driver"""
    id = 'tebi'
    default_kwargs = {
        'endpoint_url': "https://s3.tebi.io",
    }

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.default_kwargs['endpoint_url'], bucket_id, name)
        return url
