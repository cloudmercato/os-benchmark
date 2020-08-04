"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  backblaze:
    driver: backblaze_s3
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    endpoint_url: s3.<zone_id>.backblazeb2.com

Possible zone IDs are the following:

- ``us-west-001``
- ``us-west-002``
- ``eu-central-003``

.. _boto3: https://github.com/boto/boto3
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Backblaze S3 Driver"""
    id = 'exoscale'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url

    def delete_object(self, bucket_id, name, **kwargs):
        versions = self.s3.meta.client.list_object_versions(
            Bucket=bucket_id, KeyMarker=name).get('DeleteMarkers', [])
        obj = self.s3.Object(bucket_id, name)
        for version in versions:
            if version['Key'] == name:
                self.logger.debug('Remove %s version %s', name, version['VersionId'])
                obj.delete(VersionId=version['VersionId'])
