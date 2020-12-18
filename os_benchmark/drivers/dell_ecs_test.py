"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  dell:
    driver: ecs_test_s3
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Dell EMC ECS test S3 Driver"""
    id = 'dell_ecs_test'
    endpoint_url = 'https://object.ecstestdrive.com'
    default_kwargs = {
        'endpoint_url': endpoint_url,
    }

    def get_url(self, bucket_id, name, **kwargs):
        url = 'https://%s.public.ecstestdrive.com/%s/%s' % (
            self.kwargs['aws_access_key_id'].split('@')[0],
            bucket_id,
            name
        )
        return url
