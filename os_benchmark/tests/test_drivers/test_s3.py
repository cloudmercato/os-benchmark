from unittest import TestCase, mock
from io import BytesIO

from os_benchmark.drivers import s3
from os_benchmark.drivers import errors

import boto3
import botocore
from botocore.stub import Stubber
from botocore import exceptions
from moto import mock_s3


def mock_connection_error(func):
    def decorator(func):
        def wrapper(*args, **kwargs):
            def mock_operation(self, operation_name, kwarg):
                raise exceptions.ConnectTimeoutError()
            with mock.patch(
                'botocore.client.BaseClient._make_api_call',
                mock_operation
            ):
                return mock_s3(func(*args, **kwargs))
        return wrapper
    return decorator


class BaseS3Test(TestCase):
    def setUp(self):
        self.driver = s3.Driver()


class S3Test(TestCase):
    def test_property(self):
        driver = s3.Driver()
        self.assertIsInstance(
            driver.s3,
            boto3.resources.factory.ServiceResource
        )

    def test_params(self):
        endpoint_url = "https://s3.cloud-mercato.com"
        driver = s3.Driver(
            endpoint_url=endpoint_url,
            connect_timeout=52,
            read_timeout=42,
        )
        self.assertEqual(driver.s3.meta.client._client_config.connect_timeout, 52)
        self.assertEqual(driver.s3.meta.client._client_config.read_timeout, 42)
        self.assertEqual(driver.s3.meta.client._endpoint.host, endpoint_url)


class S3CreateBucketTest(BaseS3Test):
    @mock_s3
    def test_create_bucket(self):
        bucket = self.driver.create_bucket(
            name='foo',
        )
        self.assertIsInstance(bucket, dict)
        self.assertEqual(bucket['id'], 'foo')

    @mock_s3
    @mock_connection_error
    def test_create_bucket_connect_timeout(self):
        self.driver.create_bucket(name='foo')
        self.assertRaises(
            errors.DriverConnectionError,
            self.driver.create_bucket,
            name='foo',
        )

    def test_unknown_error_code(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('create_bucket')
            self.assertRaises(
                botocore.exceptions.ClientError,
                self.driver.create_bucket,
                name='foo',
            )

    def test_not_implemented(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('create_bucket', service_error_code='NotImplemented')
            self.assertRaises(
                errors.DriverFeatureUnsupported,
                self.driver.create_bucket,
                name='foo',
            )


class S3ListBucketsTest(BaseS3Test):
    @mock_s3
    def test_create_bucket(self):
        buckets = self.driver.list_buckets()
        self.assertIsInstance(buckets, list)

    @mock_s3
    def test_list_buckets(self):
        # Setup
        self.driver.create_bucket('foo')
        # Test
        buckets = self.driver.list_buckets()
        self.assertTrue(buckets)
        self.assertEqual(buckets[0]['id'], 'foo')


class S3DeleteBucketTest(BaseS3Test):
    @mock_s3
    def test_delete_bucket(self):
        # Setup
        bucket_id = 'foo'
        self.driver.create_bucket(bucket_id)
        # Test
        self.driver.delete_bucket(bucket_id=bucket_id)

    @mock_s3
    def test_bucket_not_exist(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('delete_bucket', 'NoSuchBucket')
            self.driver.delete_bucket(bucket_id='foo')

    @mock_s3
    def test_bucket_not_empty(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('delete_bucket', 'BucketNotEmpty')
            self.assertRaises(
                errors.DriverNonEmptyBucketError,
                self.driver.delete_bucket,
                bucket_id='foo',
            )


class S3ListObjectsTest(BaseS3Test):
    @mock_s3
    def test_list_objects(self):
        # Setup
        bucket_id = 'foo'
        self.driver.create_bucket(bucket_id)
        # Test
        objs = self.driver.list_objects(bucket_id=bucket_id)
        self.assertIsInstance(objs, list)

    @mock_s3
    def test_bucket_not_exist(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('list_objects', 'NoSuchBucket')
            self.assertRaises(
                errors.DriverBucketUnfoundError,
                self.driver.list_objects,
                bucket_id='foo',
            )


class S3UploadObjectTest(BaseS3Test):
    @mock_s3
    def test_upload(self):
        # Setup
        bucket_id = 'foo'
        self.driver.create_bucket(bucket_id)
        # Test
        obj = self.driver.upload(
            bucket_id=bucket_id,
            name='bar',
            content=BytesIO(),
        )
        self.assertIsInstance(obj, dict)

    @mock_s3
    def test_bucket_not_exist(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('put_object', 'NoSuchBucket')
            self.assertRaises(
                errors.DriverBucketUnfoundError,
                self.driver.upload,
                bucket_id='foo',
                name='bar',
                content=BytesIO(),
            )

    @mock_s3
    def test_bucket_access_denied(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('put_object', 'AccessDenied')
            self.assertRaises(
                errors.DriverBucketUnfoundError,
                self.driver.upload,
                bucket_id='foo',
                name='bar',
                content=BytesIO(),
            )

    @mock_s3
    def test_unknown_error(self):
        with Stubber(self.driver.s3.meta.client) as stubber:
            stubber.add_client_error('put_object', 'Foo')
            self.assertRaises(
                botocore.exceptions.ClientError,
                self.driver.upload,
                bucket_id='foo',
                name='bar',
                content=BytesIO(),
            )


class S3DeleteObjectTest(BaseS3Test):
    @mock_s3
    def test_delete_object(self):
        # Setup
        bucket_id = 'foo'
        object_id = 'bar'
        self.driver.create_bucket(bucket_id)
        self.driver.upload(
            bucket_id=bucket_id,
            name=object_id,
            content=BytesIO(),
        )
        # Test
        self.driver.delete_object(
            bucket_id=bucket_id,
            name=object_id,
        )


class S3DeleteObjectsTest(BaseS3Test):
    @mock_s3
    def test_delete_objects(self):
        # Setup
        bucket_id = 'foo'
        object1_id = 'bar'
        object2_id = 'ham'
        self.driver.create_bucket(bucket_id)
        self.driver.upload(
            bucket_id=bucket_id,
            name=object1_id,
            content=BytesIO(),
        )
        self.driver.upload(
            bucket_id=bucket_id,
            name=object2_id,
            content=BytesIO(),
        )
        # Test
        self.driver.delete_objects(
            bucket_id=bucket_id,
            names=[object1_id, object2_id],
        )

    @mock_s3
    def test_empty_names(self):
        self.driver.delete_objects(
            bucket_id='foo',
            names=[]
        )


class S3CopyObjectTest(BaseS3Test):
    @mock_s3
    def test_delete_object(self):
        # Setup
        bucket_id = 'foo'
        object_id = 'bar'
        object2_id = 'ham'
        self.driver.create_bucket(bucket_id)
        self.driver.upload(
            bucket_id=bucket_id,
            name=object_id,
            content=BytesIO(),
        )
        # Run
        self.driver.copy_object(
            bucket_id=bucket_id,
            name=object_id,
            dst_bucket_id=bucket_id,
            dst_name=object2_id,
        )
        # Test
        objs = self.driver.list_objects(bucket_id)
        self.assertIn(object2_id, objs)


class S3GeneratePresignedUrlTest(BaseS3Test):
    @mock_s3
    def test_func(self):
        # Setup
        bucket_id = 'foo'
        object_id = 'bar'
        self.driver.create_bucket(bucket_id)
        self.driver.upload(
            bucket_id=bucket_id,
            name=object_id,
            content=BytesIO(),
        )
        # Run
        url = self.driver.get_presigned_url(
            bucket_id=bucket_id,
            name=object_id,
        )
        # Test
        self.assertTrue(url.startswith('https://foo.s3.amazonaws.com/bar'))


class S3GetEndpointUrlTest(BaseS3Test):
    def tearDown(self):
        s3.Driver.default_kwargs = {}
        s3.Driver.endpoint_url = None

    @mock_s3
    def test_default(self):
        url = self.driver.get_endpoint_url()
        self.assertEqual(url, self.driver.s3.meta.client._endpoint.host)

    @mock_s3
    def test_kwargs(self):
        endpoint_url = "https://s3.cloud-mercato.com"
        self.driver = s3.Driver(endpoint_url=endpoint_url)
        url = self.driver.get_endpoint_url()
        self.assertEqual(url, endpoint_url)

    @mock_s3
    def test_default_kwargs(self):
        endpoint_url = "https://def.s3.cloud-mercato.com"
        self.driver = s3.Driver.default_kwargs['endpoint_url'] = endpoint_url
        self.driver = s3.Driver()
        url = self.driver.get_endpoint_url()
        self.assertEqual(url, endpoint_url)

    @mock_s3
    def test_endpoint_url_attributes(self):
        endpoint_url = "https://attr.s3.cloud-mercato.com"
        self.driver = s3.Driver.endpoint_url = endpoint_url
        self.driver = s3.Driver()
        self.driver.endpoint_url = endpoint_url
        url = self.driver.get_endpoint_url()
        self.assertEqual(url, endpoint_url)


class S3GetUrlTest(BaseS3Test):
    def tearDown(self):
        self.driver.default_kwargs = {}
        self.driver.endpoint_url = None

    @mock_s3
    def test_presigned(self):
        # Setup
        bucket_id = 'foo'
        object_id = 'bar'
        self.driver.create_bucket(bucket_id)
        self.driver.upload(
            bucket_id=bucket_id,
            name=object_id,
            content=BytesIO(),
        )
        # Run
        url = self.driver.get_url(
            bucket_id=bucket_id,
            name=object_id,
            presigned=True
        )
        # Test
        self.assertTrue(url.startswith('https://foo.s3.amazonaws.com/bar'))

    @mock_s3
    def test_not_presigned(self):
        # Setup
        bucket_id = 'foo'
        object_id = 'bar'
        self.driver.create_bucket(bucket_id)
        self.driver.upload(
            bucket_id=bucket_id,
            name=object_id,
            content=BytesIO(),
        )
        # Run
        url = self.driver.get_presigned_url(
            bucket_id=bucket_id,
            name=object_id,
            presigned=False
        )
        # Test
        self.assertTrue(url.startswith('https://foo.s3.amazonaws.com/bar'))
