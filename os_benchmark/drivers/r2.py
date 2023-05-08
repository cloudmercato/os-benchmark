"""
.. note::
  This driver requires `boto3`_.

.. note::
  CloudFlare R2 doesn't follow the `public-read` ACL, nor policy,
  So we use presigned-URLs

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  r2:
    driver: r2
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: https://<accountid>.r2.cloudflarestorage.com'

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Cloudflaore R2 S3 Driver"""
    id = 'r2'
    default_config = {
        'signature_version': 's3v4',
    }

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        if not presigned:
            self.logger.warn("Public read URL isn't available, forcing presigned.")
        url = self.get_presigned_url(bucket_id, name)
        return url
