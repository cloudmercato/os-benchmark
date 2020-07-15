"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  linode:
    driver: linode
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://<zone_id>.linodeobjects.com

Possible zone IDs are the following:

- ``us-east-1``: Newark, NJ
- ``eu-central-1``: Frankfurt, DE
- ``ap-south-1``: Singapore, SG

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Linode S3 Driver"""
    id = 'linode'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
