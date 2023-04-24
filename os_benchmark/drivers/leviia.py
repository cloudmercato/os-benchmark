"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  leviia:
    driver: leviia
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Leviia S3 Driver"""
    id = 'leviia'
    public_endpoint_template = "https://%s.cdn.leviia.com/"
    default_kwargs = {
        'endpoint_url': 'https://s3.leviia.com'
    }

    def get_url(self, bucket_id, name, endpoint_type='public', **kwargs):
        self.enable_bucket_website(bucket_id)
        url = self.public_endpoint_template % bucket_id
        url += name
        return url
