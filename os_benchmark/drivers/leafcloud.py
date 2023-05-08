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
    default_config = {
        's3': {
            'addressing_style': 'auto',
        }
    }

    def get_url(self, bucket_id, name, endpoint_type='public', **kwargs):
        url = super().get_url(bucket_id, name, endpoint_type='public', **kwargs)
        if endpoint_type == 'public':
            self.put_bucket_policy(bucket_id)
        return url
