"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  infomaniak:
    driver: infomaniak
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Infomaniak S3 Driver"""
    id = 'infomaniak'
    default_kwargs = {
        'endpoint_url': 'https://s3.pub1.infomaniak.cloud/',
    }

    def get_url(self, bucket_id, name, endpoint_type='public', presigned=True, **kwargs):
        if not presigned:
            self.logger.warn("Public object without presigned isn't available")
        return super().get_url(bucket_id, name, presigned=True, **kwargs)
