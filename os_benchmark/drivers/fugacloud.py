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

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """FugaCloud S3 Driver"""
    id = 'fugacloud'
    default_kwargs = {
        'endpoint_url': 'https://core.fuga.cloud:8080',
    }

    def __init__(self, *args, **kwargs):
        self.project_id = kwargs.pop('project_id', None)
        super().__init__(*args, **kwargs)

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        if not presigned:
            endpoint_url = self.get_endpoint_url()
            path = f"/{self.project_id}:{bucket_id}/{name}"
            return self.urljoin(endpoint_url, path)
        return super().get_url(bucket_id, name, presigned=presigned, **kwargs)
