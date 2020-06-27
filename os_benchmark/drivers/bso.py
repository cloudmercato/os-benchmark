"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  bso:
    driver: bso
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>

.. _boto3: https://github.com/boto/boto3
"""
from urllib.parse import urljoin
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """BSO-ST Driver"""
    endpoint_url = 'https://eu.bso.st'
    default_kwargs = {
        'endpoint_url': endpoint_url,
        'config': {
            'signature_version': 's3v4',
        }
    }

    def get_url(self, bucket_id, name, **kwargs):
        url = urljoin(self.endpoint_url, '%s/%s' % (bucket_id, name))
        return url
