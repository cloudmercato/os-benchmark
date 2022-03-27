from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base


class PutBucketTagTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )

    def run(self):
        self.driver.put_bucket_tags(
            bucket_id=self.bucket['id'],
            tags={'foo': 'bar'},
        )
        tags = self.driver.list_bucket_tags(
            bucket_id=self.bucket['id'],
        )
        if 'foo' not in tags:
            msg = "Bucket tagging not effective"
            raise driver_errors.DriverFeatureNotImplemented(msg)
        if tags['foo'] != 'bar':
            msg = "Wrong bucket tagging"
            raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])
