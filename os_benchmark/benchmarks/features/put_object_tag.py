from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base


class PutObjectTagTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )
        content = utils.get_random_content(1)
        self.driver.upload(
            bucket_id=self.bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.driver.put_object_tags(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
            tags={'foo': 'bar'},
        )
        tags = self.driver.list_object_tags(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
        )
        if 'foo' not in tags:
            msg = "Object tagging not effective"
            raise driver_errors.DriverFeatureNotImplemented(msg)
        if tags['foo'] != 'bar':
            msg = "Wrong object tagging"
            raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])
