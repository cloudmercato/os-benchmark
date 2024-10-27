"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  ovh_perf:
    driver: ovh_std
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://s3.<region_id>.io.cloud.ovh.net/
    region_name: <region_id>

.. _boto3: https://github.com/boto/boto3
"""
from urllib.parse import urlparse
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Standard Object Storage Driver"""
    id = 'ovhcloud_std'
    default_object_acl = 'public-read'

    def get_url(self, bucket_id, name, **kwargs):
        netloc = urlparse(self.kwargs['endpoint_url']).netloc
        url = 'https://%s.%s/%s' % (
            bucket_id,
            netloc,
            name,
        )
        return url

