"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  leafcloud:
    driver: leafcloud
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://leafcloud.store/

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Leafcloud S3 Driver"""
    id = 'leafcloud'
    default_kwargs = {
        'endpoint_url': 'https://leafcloud.store/',
    }

    def get_url(self, bucket_id, name, endpoint_type='public', presigned=True, **kwargs):
        if not presigned:
            self.logger.warn("Public object without presigned isn't available")
        return super().get_url(bucket_id, name, presigned=True, **kwargs)
