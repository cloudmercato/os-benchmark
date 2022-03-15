"""
.. note::
  This driver requires `boto3`_.

Base S3 driver allowing usage of any S3-based storage.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  s3:
    driver: s3
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    region: eu-west-1

All parameters except ``driver`` will be passed to ``boto3.resource``.
"""
from functools import wraps
from urllib.parse import urljoin

import botocore
import boto3
from boto3.s3.transfer import TransferConfig

from os_benchmark.drivers import base, errors


def handle_request(method):
    @wraps(method)
    def _handle_request(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except botocore.exceptions.ConnectionClosedError as err:
            raise errors.DriverConnectionError(err)
        except botocore.exceptions.EndpointConnectionError as err:
            raise errors.DriverConnectionError(err)
        except botocore.exceptions.ReadTimeoutError as err:
            raise errors.DriverConnectionError(err)
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']

            if 'Message' not in err.response['Error']:
                raise errors.DriverConnectionError(err)

            msg = err.response['Error']['Message']
            if code == '504':
                raise errors.DriverConnectionError(err)
            if code == 'ServiceUnavailable':
                raise errors.DriverConnectionError(err)
            if code == 'InvalidAccessKeyId':
                msg += " (endpoint: %s)" % self.s3.meta.client._endpoint.host
                raise errors.DriverAuthenticationError(msg)
            raise
    return _handle_request


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 's3'
    default_acl = 'public-read'
    default_object_acl = 'public-read'
    default_kwargs = {}
    default_config = {}
    _default_config = {
        'user_agent': base.USER_AGENT,
        'retries': {'max_attempts': 0},
        'connect_timeout': base.CONNECT_TIMEOUT,
        'read_timeout': base.READ_TIMEOUT,
        'parameter_validation': False,
        # 'max_pool_connections': self.num_thread,
        # 'proxies': proxies,
    }

    @property
    def s3(self):
        if not hasattr(self, '_s3'):
            kwargs = self.kwargs.copy()
            kwargs.update(self.default_kwargs)
            kwargs.update(self.get_custom_kwargs(kwargs))

            config = self._default_config.copy()
            config.update(self.default_config)
            config.update(kwargs.pop('config', None) or {})
            if self.read_timeout is not None:
                config['read_timeout'] = self.read_timeout
            if self.connect_timeout is not None:
                config['connect_timeout'] = self.connect_timeout
            self.logger.debug("boto Config: %s", config)
            kwargs['config'] = botocore.client.Config(**config)

            self._s3 = boto3.resource('s3', **kwargs)
        return self._s3

    def get_custom_kwargs(self, kwargs):
        return kwargs

    @handle_request
    def list_buckets(self, **kwargs):
        raw_buckets = self.s3.buckets.all()
        buckets = [{'id': b.name} for b in raw_buckets]
        return buckets

    def _get_create_request_params(self, name, acl, **kwargs):
        return {
            'Bucket': name,
            'ACL': acl,
        }

    @handle_request
    def create_bucket(self, name, acl=None, **kwargs):
        acl = acl or self.default_acl
        params = self._get_create_request_params(name=name, acl=acl, **kwargs)
        self.logger.debug('Create bucket params: %s', params)
        bucket = self.s3.create_bucket(**params)
        return {'id': name}

    @handle_request
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
            raise

    @handle_request
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

    @handle_request
    def upload(self, bucket_id, name, content, acl=None,
               multipart_threshold=None, multipart_chunksize=None,
               max_concurrency=None,
               **kwargs):
        acl = acl or self.default_object_acl
        extra = {'ACL': acl}
        multipart_threshold = multipart_threshold or base.MULTIPART_THRESHOLD
        multipart_chunksize = multipart_chunksize or base.MULTIPART_CHUNKSIZE
        max_concurrency = max_concurrency or base.MAX_CONCURRENCY

        transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
        )
        try:
            self.s3.meta.client.upload_fileobj(
                Fileobj=content,
                Bucket=bucket_id,
                Key=name,
                ExtraArgs=extra,
                Config=transfer_config,
            )
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code == 'NoSuchBucket':
                raise errors.DriverBucketUnfoundError(msg)
            elif code == 'AccessDenied':
                raise errors.DriverBucketUnfoundError(msg)
            raise
        return {'name': name}

    @handle_request
    def delete_object(self, bucket_id, name, **kwargs):
        obj = self.s3.Object(bucket_id, name)
        obj.delete()

    @handle_request
    def copy_object(self, bucket_id, name, dst_bucket_id, dst_name, **kwargs):
        copy_source = {
            'Bucket': bucket_id,
            'Key': name,
        }
        extra_args = {}
        obj = self.s3.meta.client.copy(
            CopySource=copy_source,
            Bucket=dst_bucket_id,
            Key=dst_name,
            ExtraArgs=extra_args,
        )
        return obj

    @handle_request
    def get_presigned_url(self, bucket_id, name, expiration=3600, **kwargs):
        url = self.s3.meta.client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_id, 'Key': name},
            ExpiresIn=expiration
        )
        return url

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        if presigned:
            url = self.get_presigned_url(bucket_id, name)
        else:
            url = urljoin(self.kwargs['endpoint_url'], '%s/%s' % (bucket_id, name))
        return url
