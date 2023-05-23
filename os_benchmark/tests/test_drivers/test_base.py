import io
from unittest import TestCase, mock
from os_benchmark.drivers import base, errors


class MultiPartTest(TestCase):
    def test_init(self):
        fd = io.BytesIO(b'a')
        base.MultiPart(fd, 1)

    def test_read(self):
        fd = io.BytesIO(b'abc')
        part = base.MultiPart(fd, 3)
        self.assertEqual(part.read(), b'abc')
        self.assertEqual(part.read(), '')

    def test_read_chunksize(self):
        fd = io.BytesIO(b'abc')
        part = base.MultiPart(fd, 3)
        self.assertEqual(part.read(chunksize=1), b'a')
        self.assertEqual(part.read(chunksize=1), b'b')
        self.assertEqual(part.read(chunksize=1), b'c')
        self.assertEqual(part.read(), '')

    def test_seek(self):
        fd = io.BytesIO(b'abc')
        part = base.MultiPart(fd, 3)
        part.seek(1)
        self.assertEqual(len(part.read()), 2)
        self.assertEqual(part.read(), '')

    def test_len(self):
        fd = io.BytesIO(b'abc')
        part = base.MultiPart(fd, 3)
        self.assertEqual(part.size, 3)


class MultiPartUploaderTest(TestCase):
    def test_init(self):
        base.MultiPartUploader(
            content=io.BytesIO(b'abc'),
            max_concurrency=32,
            multipart_chunksize=2**30,
        )

    def test_run(self):
        parts = []

        def _upload_func(part_id, offset, content):
            self.assertNotIn(part_id, parts)
            parts.append(part_id)
            return content.read(1)

        content = io.BytesIO(''.join([chr(i) for i in range(97, 123)]).encode())
        content.size = 26
        uploader = base.MultiPartUploader(
            content=content,
            max_concurrency=16,
            multipart_chunksize=1,
        )
        results = uploader.run(_upload_func)
        self.assertEqual(len(results), 26)
        self.assertEqual(len(parts), 26)


class BaseDriverTest(TestCase):
    def test_init(self):
        base.BaseDriver()

    def test_urljoin(self):
        driver = base.BaseDriver()
        self.assertEqual(
            driver.urljoin('https://cloud-mercato.com/', 'home'),
            'https://cloud-mercato.com/home'
        )
        self.assertEqual(
            driver.urljoin('https://cloud-mercato.com', 'home'),
            'https://cloud-mercato.com/home'
        )
        self.assertEqual(
            driver.urljoin('https://cloud-mercato.com', '/home'),
            'https://cloud-mercato.com/home'
        )

    def test_prepare_delete_objects(self):
        driver = base.BaseDriver()
        names = ['foo', 'bar', 'ham']
        result = driver.prepare_delete_objects(names)
        self.assertEqual(names, result)

    @mock.patch('os_benchmark.drivers.base.BaseDriver.delete_object')
    def test_delete_objects(self, mock_delete):
        driver = base.BaseDriver()
        names = ['foo', 'bar', 'ham']
        driver.delete_objects(bucket_id='cm', names=names)
        self.assertEqual(mock_delete.call_count, 3)

    @mock.patch(
        'os_benchmark.drivers.base.BaseDriver.list_buckets',
        return_value=[{'id': 'foo'}, {'id': 'bar'}]
    )
    def test_get_bucket(self, mock_list):
        driver = base.BaseDriver()
        driver.get_bucket(bucket_id='foo')
        self.assertEqual(mock_list.call_count, 1)

    @mock.patch(
        'os_benchmark.drivers.base.BaseDriver.list_buckets',
        return_value=[],
    )
    def test_get_bucket_not_found(self, mock_list):
        driver = base.BaseDriver()
        self.assertRaises(
            errors.DriverBucketUnfoundError,
            driver.get_bucket,
            bucket_id='foo'
        )
        self.assertEqual(mock_list.call_count, 1)

    @mock.patch(
        'os_benchmark.drivers.base.BaseDriver.list_objects',
        return_value=['foo', 'bar'],
    )
    def test_get_object(self, mock_list):
        driver = base.BaseDriver()
        driver.get_object(bucket_id='cm', name='foo')
        self.assertEqual(mock_list.call_count, 1)

    @mock.patch(
        'os_benchmark.drivers.base.BaseDriver.list_objects',
        return_value=[],
    )
    def test_get_object_not_found(self, mock_list):
        driver = base.BaseDriver()
        self.assertRaises(
            errors.DriverObjectUnfoundError,
            driver.get_object,
            bucket_id='cm',
            name='foo',
        )
        self.assertEqual(mock_list.call_count, 1)
