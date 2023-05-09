"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  cloudferro:
    driver: cloudferro
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://s3.waw3-1.cloudferro.com/

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """CloudFerro S3 Driver"""
    id = 'cloudferro'

    def get_url(self, bucket_id, name, **kwargs):
        path = '/'.join([bucket_id, name])
        url = self.urljoin(self.kwargs['endpoint_url'], path)
        return url
