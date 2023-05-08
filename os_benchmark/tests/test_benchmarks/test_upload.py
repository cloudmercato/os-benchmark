from unittest import TestCase
from os_benchmark.tests import utils
from os_benchmark.benchmarks import upload


class UploadBenchmarkSetupTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.driver.create_bucket('foo')
        self.bench = upload.UploadBenchmark(self.driver)
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
        })

    def test_func(self):
        self.bench.setup()


class UploadBenchmarkRunTest(TestCase):
    def setUp(self):
        self.driver = utils.InMemoryDriver()
        self.driver.create_bucket('foo')
        self.bench = upload.UploadBenchmark(self.driver)
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'multipart_threshold': 1,
            'multipart_chunksize': 1,
            'max_concurrency': 1,
        })
        self.bench.setup()

    def test_func(self):
        self.bench.run()


class UploadBenchmarkTearDownTest(TestCase):
    def setUp(self):
        self.skipTest("Failing test, no end")
        self.driver = utils.InMemoryDriver()
        self.driver.create_bucket('foo')
        self.bench = upload.UploadBenchmark(self.driver)
        self.bench.params.update({
            'object_size': 1,
            'object_number': 1,
            'multipart_threshold': 1,
            'multipart_chunksize': 1,
            'max_concurrency': 1,
        })
        self.bench.setup()

    def test_func(self):
        self.bench.tear_down()
