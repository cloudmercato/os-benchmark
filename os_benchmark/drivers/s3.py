"""
.. note::
  This driver requires `boto3`_.

Base S3 driver using boto3 allowing usage of any S3-based storage.

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
from datetime import datetime, timedelta
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
            if code == 'NoSuchBucket':
                raise errors.DriverBucketUnfoundError(msg)
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

    def set_backend_logger(self, verbosity):
        if verbosity == 4:
            boto3.set_stream_logger('botocore')

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
    def create_bucket(self, name, acl=None, bucket_lock=None, **kwargs):
        acl = acl or self.default_acl
        params = self._get_create_request_params(name=name, acl=acl, **kwargs)
        if bucket_lock is not None:
            params['ObjectLockEnabledForBucket'] = bucket_lock

        self.logger.debug('Create bucket params: %s', params)
        bucket = self.s3.create_bucket(**params)
        return {'id': name}

    @handle_request
    def delete_bucket(self, bucket_id, **kwargs):
        try:
            self.s3.meta.client.delete_bucket_policy(Bucket=bucket_id)
        except Exception as err:
            self.logger.debug(err)

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
        try:
            response = self.s3.meta.client.list_objects(
                Bucket=bucket_id,
            )
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code == 'NoSuchBucket':
                raise errors.DriverBucketUnfoundError(msg)
            raise
        objects = [o['Key'] for o in response.get('Contents', [])]
        return objects

    @handle_request
    def put_bucket_cors(self, bucket_id, **kwargs):
        allowed_methods = ['GET', 'POST', 'HEAD']
        expose_headers = [
            'Access-Control-Allow-Origin',
        ]
        config = {
            'CORSRules': [
                {
                    'AllowedMethods': allowed_methods,
                    'AllowedOrigins': ['*'],
                    'AllowedHeaders': ['*'],
                    'ExposeHeaders': expose_headers,
                    'MaxAgeSeconds': 3600,
                }
            ]
        }
        params = {
            'Bucket': bucket_id,
            'CORSConfiguration': config
        }
        self.logger.debug("CORS params: %s", params)
        try:
            response = self.s3.meta.client.put_bucket_cors(**params)
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code == 'NoSuchBucket':
                raise errors.DriverBucketUnfoundError(msg)
            raise

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
    def delete_object(self, bucket_id, name, skip_lock=None, version_id=None, **kwargs):
        params = {
            'Bucket': bucket_id,
            'Key': name,
        }
        if skip_lock is not None:
            params['BypassGovernanceRetention'] = skip_lock
        if version_id is not None:
            params['VersionId'] = version_id
        self.logger.debug('Delete object params: %s', params)
        try:
            self.s3.meta.client.delete_object(**params)
        except botocore.exceptions.ClientError as err:
            raise

    def prepare_delete_objects(self, bucket_id, names, skip_lock=None,
                               **kwargs):
        request = {
            'Bucket': bucket_id,
            'Delete': {
                'Objects': [{'Key': n} for n in names],
            }
        }
        if skip_lock is not None:
            request['BypassGovernanceRetention'] = skip_lock
        return request

    @handle_request
    def delete_objects(self, bucket_id, names, skip_lock=None, request=None, **kwargs):
        if not names:
            self.logger.debug("Skip empty list name")
            return

        if request is None:
            request = self.prepare_delete_objects(
                bucket_id=bucket_id,
                names=names,
                skip_lock=skip_lock,
            )
        self.logger.debug("Delete objects params: %s", request)
        self.s3.meta.client.delete_objects(**request)

    @handle_request
    def copy_object(self, bucket_id, name, dst_bucket_id, dst_name, **kwargs):
        copy_source = {
            'Bucket': bucket_id,
            'Key': name,
        }
        extra_args = {}
        try:
            obj = self.s3.meta.client.copy(
                CopySource=copy_source,
                Bucket=dst_bucket_id,
                Key=dst_name,
                ExtraArgs=extra_args,
            )
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            if code == '404':
                amz_code = err.response['ResponseMetadata']['HTTPHeaders']['x-amz-error-code']
                msg = err.response['ResponseMetadata']['HTTPHeaders']['x-amz-error-message']
                if amz_code == 'NoSuchBucket':
                    msg = '%s %s' % (msg, bucket_id)
                    raise errors.DriverBucketUnfoundError(msg)
                raise errors.DriverObjectUnfoundError(msg)
            raise
        return obj

    @handle_request
    def put_object_tags(self, bucket_id, name, tags, **kwargs):
        msg = "PutObjectTagging doesn't work with boto3"
        raise NotImplementedError(msg)

        try:
            params = {
                'Bucket': bucket_id,
                'Key': name,
                'Tagging': {
                    'TagSet': [{
                        'Key': key,
                        'Value': value,
                    } for key, value in tags.items()]
                }
            }
            self.logger.debug("Put tag params: %s", params)
            self.s3.meta.client.put_object_tagging(**params)
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            if code == 'InvalidArgument' and err.response['Error']['ArgumentName'] == 'tagging':
                msg = err.args[0]
                raise errors.DriverFeatureNotImplemented(msg)
            raise

    @handle_request
    def list_object_tags(self, bucket_id, name, **kwargs):
        try:
            raw_tags = self.s3.meta.client.get_object_tagging(
                Bucket=bucket_id,
                Key=name,
            )
        except botocore.exceptions.ClientError as err:
            raise
        return {t['Key']: t['Value'] for t in raw_tags['TagSet']}

    @handle_request
    def put_object_lock(self, bucket_id, name, **kwargs):
        mode = 'GOVERNANCE'
        retain_date = datetime.now() + timedelta(hours=1)
        try:
            response = self.s3.meta.client.put_object_retention(
                Bucket=bucket_id,
                Key=name,
                Retention={'Mode': mode, 'RetainUntilDate': retain_date}
            )
        except botocore.exceptions.ClientError as err:
            raise

    def list_objects_versions(self, bucket_id, **kwargs):
        params = {'Bucket': bucket_id}
        try:
            response = self.s3.meta.client.list_object_versions(**params)
        except botocore.exceptions.ClientError as err:
            raise
        return [{
            'id': v['VersionId'],
            'bucket_id': bucket_id,
            'name': v['Key'],
        } for v in response.get('Versions', [])]

    def list_delete_markers(self, bucket_id, **kwargs):
        params = {'Bucket': bucket_id}
        try:
            response = self.s3.meta.client.list_object_versions(**params)
        except botocore.exceptions.ClientError as err:
            raise
        return [{
            'id': v['VersionId'],
            'bucket_id': bucket_id,
            'name': v['Key'],
        } for v in response.get('DeleteMarkers', [])]

    def list_object_versions(self, bucket_id, name, **kwargs):
        params = {
            'Bucket': bucket_id,
            'Prefix': name,
        }
        try:
            response = self.s3.meta.client.list_object_versions(**params)
        except botocore.exceptions.ClientError as err:
            raise
        return [{
            'id': v['VersionId'],
            'bucket_id': bucket_id,
            'name': v['Key'],
        } for v in response.get('Versions', [])]

    def list_multipart_uploads(self, bucket_id, **kwargs):
        params = {'Bucket': bucket_id}
        try:
            response = self.s3.meta.client.list_multipart_uploads(**params)
        except botocore.exceptions.ClientError as err:
            raise
        return [{
            'id': m['UploadId'],
            'bucket_id': bucket_id,
            'name': m['Key'],
        } for m in response.get('Uploads', [])]

    def delete_multipart_upload(self, bucket_id, name, upload_id, **kwargs):
        params = {
            'Bucket': bucket_id,
            'Key': name,
            'UploadId': upload_id
        }
        try:
            response = self.s3.meta.client.abort_multipart_upload(**params)
        except botocore.exceptions.ClientError as err:
            raise

    @handle_request
    def get_object_torrent(self, bucket_id, name, **kwargs):
        try:
            response = self.s3.meta.client.get_object_torrent(
                Bucket=bucket_id,
                Key=name,
            )
        except botocore.exceptions.ClientError as err:
            code = err.response['Error']['Code']
            msg = err.response['Error']['Message']
            if code in ('NotImplemented', 'MethodNotAllowed'):
                raise errors.DriverFeatureNotImplemented(msg)
            raise
        magnet = response['Body'].read()
        if len(magnet) <= 1:
            msg = "Empty magnet"
            raise errors.DriverFeatureNotImplemented(msg)
        return magnet

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
