"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  tencent:
    driver: tencent_s3
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    app_id: <your_app_id>
    endpoint_url: https://cos.<zone_id>.myqcloud.com

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Tencent S3 Driver"""
    id = 'tencent_s3'

    default_config = {
        's3': {
            'addressing_style': 'virtual',
        }
    }

    def __init__(self, *args, **kwargs):
        self.app_id = kwargs.pop('app_id', None)
        super().__init__(*args, **kwargs)

    def _get_create_request_params(self, **kwargs):
        params = super()._get_create_request_params(**kwargs)
        params['Bucket'] += '-%s' % self.app_id
        return params

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
