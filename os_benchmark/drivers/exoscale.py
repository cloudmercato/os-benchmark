"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  exoscale:
    driver: exoscale
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://sos-<zone_id>.exo.io

Possible zone IDs are the following:

- ``de-fra-1``: Frankfurt, Germany
- ``ch-dk-2``: Zurich, Switzerland
- ``at-vie-1``: Vienna, Austria
- ``de-muc-1``: Munich, Germany
- ``ch-gva-2``: Geneva, Switzerland
- ``bg-sof-1``: Sofia, Bulgaria

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Exoscale S3 Driver"""
    id = 'exoscale'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
