"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  oracle:
    driver: oracle_s3
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://<namespace>.compat.objectstorage.<zone_id>.oraclecloud.com

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Oracle Cloud S3 Driver"""
    id = 'oracle_s3'
    default_acl = 'public-read'
    default_object_acl = 'public-read'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
