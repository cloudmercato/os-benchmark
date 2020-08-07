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
    id = 'backblaze_s3'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url

    def delete_bucket(self, bucket_id, **kwargs):
        objs = self.s3.meta.client.list_object_versions(Bucket=bucket_id)
        versions = objs.get('Versions', [])
        del_markers = objs.get('DeleteMarkers', [])
        for version in versions:
            self.logger.info('Remove %s version %s', version['Key'], version['VersionId'])
            self.s3.meta.client.delete_object(Bucket=bucket_id, Key=version['Key'], VersionId=version['VersionId'])
        for version in del_markers:
            self.logger.info('Remove %s Delete marker %s', version['Key'], version['VersionId'])
            self.s3.meta.client.delete_object(Bucket=bucket_id, Key=version['Key'], VersionId=version['VersionId'])
        multiparts = self.s3.meta.client.list_multipart_uploads(Bucket=bucket_id)\
            .get('Uploads', [])
        for part in multiparts:
            self.logger.info('Remove %s multpart %s', part['Key'], part['UploadId'])
            self.s3.meta.client.abort_multipart_upload(
                Bucket=bucket_id, Key=part['Key'], UploadId=part['UploadId'],
            )
        super().delete_bucket(bucket_id, **kwargs)

    def delete_object(self, bucket_id, name, **kwargs):
        versions = self.s3.meta.client.list_object_versions(Bucket=bucket_id)\
            .get('Versions', [])
        obj = self.s3.Object(bucket_id, name)
        for version in versions:
            if version['Key'] == name:
                self.logger.info('Remove %s version %s', name, version['VersionId'])
                obj.delete(VersionId=version['VersionId'])
        super().delete_object(bucket_id, name, **kwargs)
