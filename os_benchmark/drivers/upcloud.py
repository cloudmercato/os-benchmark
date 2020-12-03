"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  upcloud:
    driver: upcloud
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from urllib.parse import urlparse
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """UpCloud S3 Driver"""
    id = 'upcloud'

    def get_custom_kwargs(self, kwargs):
        kwargs['endpoint_url'] = self.kwargs['endpoint_url']
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        domain = urlparse(self.kwargs['endpoint_url']).netloc
        url = 'https://%s.%s/%s' % (bucket_id, domain, name)
        return url
