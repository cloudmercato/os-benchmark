from unittest import TestCase
from os_benchmark.tests import utils
from os_benchmark.benchmarks import base


class BaseBenchmarkInitTest(TestCase):
    def test_func(self):
        driver = utils.InMemoryDriver()
        base.BaseBenchmark(driver)


class BaseBenchmarkMakeAggrTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseBenchmark(self.driver)

    def test_func(self):
        values = (12, 24, 35)
        self.bench._make_aggr(values=values)


class BaseBenchmarkTimeItTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseBenchmark(self.driver)

    def test_func(self):
        func = lambda x: x
        elapsed, output = self.bench.timeit(func, 'foo')
        self.assertGreater(elapsed, 0)
        self.assertEqual('foo', 'foo')


class BaseSetupObjectsBenchmarkMakeUploadTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseSetupObjectsBenchmark(self.driver)
        self.bench.bucket = self.driver.create_bucket('foo')
        self.bench.objects = []
        self.bench.urls = []
        self.bench.bucket_id = 'foo'
        self.bench.storage_class = 'bar'
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'presigned': False,
        })

    def test_func(self):
        self.bench._make_upload()


class BaseSetupObjectsBenchmarkCreateBucketTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseSetupObjectsBenchmark(self.driver)
        self.bench.objects = []
        self.bench.urls = []
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'presigned': False,
        })

    def test_func(self):
        self.bench._create_bucket()


class BaseSetupObjectsBenchmarkReuseBucketTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.driver.create_bucket('foo')
        self.bench = base.BaseSetupObjectsBenchmark(self.driver)
        self.bench.bucket_id = 'foo'
        self.bench.objects = []
        self.bench.urls = []
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'bucket_id': 'foo',
            'presigned': False,
        })

    def test_func(self):
        self.bench._reuse_bucket()


class BaseSetupObjectsBenchmarkSetupTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseSetupObjectsBenchmark(self.driver)
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'presigned': False,
        })

    def test_func(self):
        self.bench.setup()

    def test_reuse_bucket(self):
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'presigned': False,
            'bucket_id': 'foo',
        })
        self.bench.setup()


class BaseSetupObjectsBenchmarkTearDownTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseSetupObjectsBenchmark(self.driver)
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'presigned': False,
        })
        self.bench.bucket = {
            'id': 'foo',
        }

    def test_func(self):
        self.bench.tear_down()


class BaseNetworkBenchmarkSetupTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.bench = base.BaseNetworkBenchmark(self.driver)
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'presigned': False,
        })

    def test_func(self):
        self.bench.setup()
        self.assertEqual(self.bench.port, 443)
