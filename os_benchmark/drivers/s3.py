"""
.. note::
  This driver requires `boto3`_.

Base S3 driver allowing usage of any S3-based storage.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  s3:
    driver: s2
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    region: eu-west-1

All parameters except ``driver`` will be passed to ``boto3.resource``.
"""
import boto3
import botocore
from os_benchmark.drivers import base, errors


class Driver(base.RequestsMixin, base.BaseDriver):
    default_kwargs = {}

    @property
    def s3(self):
        if not hasattr(self, '_s3'):
            kwargs = self.kwargs.copy()
            kwargs.update(self.default_kwargs)
            kwargs.update(self.get_custom_kwargs(kwargs))
            config = kwargs.pop('config', None)
            if config is not None:
                kwargs['config'] = botocore.client.Config(**config)
            # TODO: Remove sensitive data
            # self.logger.debug("S3 config: %s", kwargs)
            self._s3 = boto3.resource('s3', **kwargs)
        return self._s3

    def get_custom_kwargs(self, kwargs):
        return kwargs

    def list_buckets(self, **kwargs):
        try:
            raw_buckets = self.s3.buckets.all()
            buckets = [{'id': b.name} for b in raw_buckets]
        except botocore.exceptions.EndpointConnectionError as err:
            raise errors.DriverConnectionError(err)
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code == 'InvalidAccessKeyId':
                msg += " (endpoint: %s)" % self.endpoint_url
                raise errors.DriverAuthenticationError(msg)
            raise
        return buckets

    def create_bucket(self, name, acl='public-read', **kwargs):
        params = {
            'Bucket': name,
            'ACL': acl,
        }
        bucket = self.s3.create_bucket(**params)
        return {'id': name}

    def delete_bucket(self, bucket_id, **kwargs):
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
            if code == 'InvalidAccessKeyId':
                msg += " (endpoint: %s)" % self.endpoint_url
                raise errors.DriverAuthenticationError(msg)
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
            if code == 'InvalidAccessKeyId':
                msg += " (endpoint: %s)" % self.endpoint_url
                raise errors.DriverAuthenticationError(msg)
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

    def delete_object(self, bucket_id, name, **kwargs):
        obj = self.s3.Object(bucket_id, name)
        obj.delete()
