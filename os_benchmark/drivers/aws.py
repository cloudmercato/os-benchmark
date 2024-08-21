"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  aws:
    driver: aws
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://s3.<region_id>.amazonaws.com
    region_name: <region_id>

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import minio_sdk


class Driver(minio_sdk.Driver):
    """AWS S3 Driver"""
    id = 'aws'
    default_acl = None
    default_object_acl = None

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
