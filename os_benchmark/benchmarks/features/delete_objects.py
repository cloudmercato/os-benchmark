from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base


class DeleteObjectsTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        base_obj_name = utils.get_random_name()
        self.objects = []
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )
        for i in range(3):
            obj_name = '%s-%s' % (base_obj_name, i)
            content = utils.get_random_content(1)
            obj = self.driver.upload(
                bucket_id=self.bucket['id'],
                storage_class=self.params['storage_class'],
                name=obj_name,
                content=content,
            )
            self.objects.append(obj)
        self.prep_request = self.driver.prepare_delete_objects(
            bucket_id=self.bucket['id'],
            names=self.objects,
        )

    def run(self):
        self.driver.delete_objects(
            bucket_id=self.bucket['id'],
            names=self.objects,
            request=self.prep_request,
        )
        for obj in self.objects:
            exists = self.driver.test_object_exists(self.bucket['id'], obj)
            if exists:
                msg = "Object deletion not effective"
                raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])
