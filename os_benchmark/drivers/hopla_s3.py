"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  hopla:
    driver: hopla
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from urllib.parse import urlparse
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Hopla.cloud S3 Driver"""
    id = 'hopla'
    endpoint_url = 'https://s3-fr-east-1.pub.hopla.cloud'
    put_object_acl = True

    def get_custom_kwargs(self, kwargs):
        kwargs['endpoint_url'] = self.endpoint_url
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        domain = urlparse(self.endpoint_url).netloc
        url = 'https://%s/%s/%s' % (domain, bucket_id, name)
        return url
