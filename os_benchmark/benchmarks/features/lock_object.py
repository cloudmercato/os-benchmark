from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base


class LockObjectTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
            bucket_lock=True,
        )
        content = utils.get_random_content(1)
        self.driver.upload(
            bucket_id=self.bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.logger.debug("Putting lock on object")
        self.driver.put_object_lock(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
        )
        try:
            self.logger.debug("Trying lock object deletion")
            self.driver.delete_object(self.bucket['id'], self.obj_name, skip_lock=False)
        except Exception as err:
            self.logger.debug(err)
        else:
            exists = self.driver.test_object_exists(
                bucket_id=self.bucket['id'],
                name=self.obj_name,
                check_version=True,
            )
            if not exists:
                msg = "Object lock not effective"
                raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'], skip_lock=True)
