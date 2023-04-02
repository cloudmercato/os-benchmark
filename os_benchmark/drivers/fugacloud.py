"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  fugacloud:
    driver: fugacloud
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    project_id: <your_project_id>
    endpoint_url: https://core.fuga.cloud:8080

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """FugaCloud S3 Driver"""
    id = 'fugacloud'

    def __init__(self, *args, **kwargs):
        self.project_id = kwargs.pop('project_id', None)
        super().__init__(*args, **kwargs)

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s:%s/%s' % (
            self.kwargs['endpoint_url'],
            self.project_id,
            bucket_id,
            name,
        )
        return url
