"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  otc:
    driver: otc
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://obs.<zone_id>.otc.t-systems.com

Possible zone IDs are the following:

- ``eu-de``: Germany
- ``eu-nl``: Netherland

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """T-Systems Open Cloud Telekom S3 Driver"""
    id = 'otc'

    def _get_create_request_params(self, *args, **kwargs):
        params = super()._get_create_request_params(*args, **kwargs)
        if 'region_name' in self.kwargs:
            params['CreateBucketConfiguration'] = {
                'LocationConstraint': self.kwargs['region_name']
            }
        return params

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
