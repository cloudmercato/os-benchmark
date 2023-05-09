"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  ionos:
    driver: ionos
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://s3-<zone_id>.ionoscloud.com

Possible `zone_id` are:::
    - eu-central-1: Frankfurt, Germany (EU Central)
    - eu-central-2: Berlin, Germany (EU Central)
    - eu-south-2: Logrono, Spain (EU South)

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """IONOS S3 Driver"""
    id = 'ionos'

    def _get_create_request_params(self, *args, **kwargs):
        endpoint_url = self.kwargs['endpoint_url']
        if not endpoint_url.startswith('https://s3-eu-central-1'):
            self.kwargs['region_name'] = endpoint_url[11:].split('.')[0]
        return super()._get_create_request_params(*args, **kwargs)

    def get_url(self, bucket_id, name, **kwargs):
        path = f"{bucket_id}/{name}"
        url = self.urljoin(self.kwargs['endpoint_url'], path)
        return url
