import requests
import boto3
import botocore
from os_benchmark.drivers import base, errors


class Driver(base.BaseDriver, base.RequestsMixin):
    @property
    def s3(self):
        if not hasattr(self, '_s3'):
            self._s3 = boto3.resource('s3', **self.kwargs)
        return self._s3

    def list_buckets(self, **kwargs):
        raw_buckets = self.s3.buckets.all()
        buckets = [
            {'id': b.name}
            for b in raw_buckets
        ]
        return buckets

    def create_bucket(self, name, acl='public-read', **kwargs):
        params = {
            'Bucket': name,
            'ACL': acl,
        }
        bucket = self.s3.create_bucket(**params)
        return {'id': name}

    def delete_bucket(self, bucket_id):
        bucket = self.s3.Bucket(bucket_id)
        try:
            bucket.delete()
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code == 'NoSuchBucket':
                self.logger.debug(err)
                return
            if code == 'BucketNotEmpty':
                raise errors.DriverNonEmptyBucketError(msg)
            raise

    def list_objects(self, bucket_id, **kwargs):
        bucket = self.s3.Bucket(bucket_id)
        try:
            objects = [o.key for o in bucket.objects.all()]
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code == 'NoSuchBucket':
                raise errors.DriverBucketUnfoundError(msg)
            raise
        return objects

    def upload(self, bucket_id, name, content, acl='public-read', **kwargs):
        extra = {'ACL': acl}
        self.s3.meta.client.upload_fileobj(
            content,
            bucket_id,
            name,
            extra,
        )
        return {'name': name}

    def download(self, url, block_size=65536, **kwargs):
        with self.session.get(url, stream=True) as response:
            for chunk in response.iter_content(chunk_size=block_size):
                pass

    def delete_object(self, bucket_id, name, **kwargs):
        obj = self.s3.Object(bucket_id, name)
        obj.delete()
