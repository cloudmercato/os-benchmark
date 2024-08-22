"""
.. note::
  This driver requires `minio`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  aws:
    driver: aws
    access_key: <your_ak>
    secret_key: <your_sk>
    endpoint: s3.<region_id>.amazonaws.com
    region: <region_id>

.. _boto3: https://github.com/boto/boto3
"""
from minio.xml import Element, SubElement, getbytes
from os_benchmark.drivers import minio_sdk


class Driver(minio_sdk.Driver):
    """AWS S3 Driver"""
    id = 'aws'
    default_object_acl = None

    def get_url(self, bucket_id, name, **kwargs):
        url = 'https://%s/%s/%s' % (self.kwargs['endpoint'], bucket_id, name)
        return url

    def _make_create_bucket_params(self, params):
        if self.kwargs['region'] == 'us-east-1':
            params['body'] = None
        elif self.kwargs.get('region'):
            element = Element("CreateBucketConfiguration")
            SubElement(element, "LocationConstraint", self.kwargs['region'])
            params['body'] = getbytes(element)
