from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base


class EnableBucketLogggingTest(base.BaseBenchmark):
    def setup(self):
        src_bucket_name = utils.get_random_name()
        dst_bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.src_bucket = self.driver.create_bucket(
            name=src_bucket_name,
            storage_class=self.params['storage_class'],
        )
        self.dst_bucket = self.driver.create_bucket(
            name=dst_bucket_name,
            storage_class=self.params['storage_class'],
        )
        content = utils.get_random_content(1)
        obj = self.driver.upload(
            bucket_id=self.src_bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.driver.enable_bucket_logging(
            bucket_id=self.src_bucket['id'],
            name=self.obj_name,
            dst_bucket_id=self.dst_bucket['id'],
        )


    def tear_down(self):
        self.driver.clean_bucket(self.src_bucket['id'])
        self.driver.clean_bucket(self.dst_bucket['id'])
