from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base

from .delete_objects import DeleteObjectsTest
from .copy_object import CopyObjectTest
from .get_torrent import GetObjectTorrentTest
from .put_object_tag import PutObjectTagTest
from .lock_object import LockObjectTest
from .cors import CorsTest
from .enable_bucket_logging import EnableBucketLogggingTest
from .put_bucket_tag import PutBucketTagTest


class Benchmark(base.BaseBenchmark):
    """Test and report feature availability"""
    TESTS = {
        'delete_objects': DeleteObjectsTest,
        'copy_object': CopyObjectTest,
        'get_torrent': GetObjectTorrentTest,
        'put_object_tag': PutObjectTagTest,
        'lock_object': LockObjectTest,
        'cors': CorsTest,
        'bucket_logging': EnableBucketLogggingTest,
        'put_bucket_tag': PutBucketTagTest,
    }

    def setup(self):
        self.params['storage_class'] = self.params.get('storage_class')
        self.tests = {}
        for test_name, test_class in self.TESTS.items():
            test = test_class(driver=self.driver)
            test.set_params(
                storage_class=self.params['storage_class'],
            )
            self.tests[test_name] = test

    def run(self, **kwargs):
        self.results = {}
        for test_name, test in self.tests.items():
            self.logger.info('Run %s', test_name)
            test.setup()
            try:
                test.run()
            except driver_errors.DriverFeatureUnsupported as err:
                self.logger.info("DriverFeatureUnsupported: %s", err)
                self.results[test_name] = 'nok'
            except NotImplementedError:
                self.results[test_name] = 'not_implemented'
            except driver_errors.DriverError as err:
                self.logger.error(err)
                self.results[test_name] = 'error'
            except Exception as err:
                self.logger.exception(err)
                self.results[test_name] = 'error'
            else:
                self.results[test_name] = 'ok'
            finally:
                try:
                    test.tear_down()
                except Exception as err:
                    self.logger.exception(err)

    def make_stats(self):
        return self.results
