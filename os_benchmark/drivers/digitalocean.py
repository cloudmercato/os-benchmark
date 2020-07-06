"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  digitalocean:
    driver: digitalocean
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://<region_id>.digitaloceanspaces.com

Possible zone IDs are the following:

- ``ams3``: Amsterdam 3, Netherland

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """DigitalOcean Spaces S3 Driver"""
    id = 'digitalocean'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
