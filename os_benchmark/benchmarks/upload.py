from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from . import base


class UploadBenchmark(base.BaseBenchmark):
    """Time objects uploading"""
    def setup(self):
        self.logger.debug("Bench params '%s'", self.params)

        self.timings = []
        self.objects = []
        self.errors = []
        self.driver.setup(**self.params)
        self.storage_class = self.params.get('storage_class')

        if self.params.get('bucket_id'):
            self.bucket_id = self.params['bucket_id']
            self.logger.debug("Reuse bucket '%s'", self.bucket_id)
            self.bucket = self.driver.get_bucket(bucket_id=self.bucket_id)
        else:
            bucket_name = utils.get_random_name(
                size=self.params.get('bucket_name_size', 30),
                prefix=self.params.get('bucket_prefix'),
                suffix=self.params.get('bucket_suffix'),
            )
            self.logger.debug("Creating bucket '%s'", bucket_name)
            self.bucket = self.driver.create_bucket(
                name=bucket_name,
                storage_class=self.storage_class,
            )
            self.bucket_id = self.bucket['id']

    def run(self, **kwargs):
        def upload_files():
            for i in range(self.params['object_number']):
                name = utils.get_random_name(
                    prefix=self.params.get('object_prefix'),
                )
                content = utils.get_random_content(self.params['object_size'])

                self.logger.debug("Uploading object '%s'", name)
                try:
                    elapsed, obj = self.timeit(
                        self.driver.upload,
                        bucket_id=self.bucket['id'],
                        storage_class=self.storage_class,
                        name=name,
                        content=content,
                        multipart_threshold=self.params['multipart_threshold'],
                        multipart_chunksize=self.params['multipart_chunksize'],
                        max_concurrency=self.params['max_concurrency'],
                    )
                    self.timings.append(elapsed)
                    self.objects.append(obj)
                except driver_errors.DriverConnectionError as err:
                    self.logger.error(err)
                    self.errors.append(err)


        self.total_time = self.timeit(upload_files)[0]

    def tear_down(self):
        if not self.params.get('keep_objects'):
            try:
                self.driver.clean_bucket(bucket_id=self.bucket['id'])
            except driver_errors.DriverNonEmptyBucketError as err:
                self.logger.error(err)

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        test_time = sum(self.timings)
        rate = (count/test_time) if test_time else 0
        bw = (total_size/test_time/2**20) if test_time else 0
        stats = {
            'operation': 'upload',
            'bucket_id': self.bucket_id,
            'ops': count,
            'time': self.total_time,
            'bw': bw,
            'rate': rate,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'multipart_threshold': self.params['multipart_threshold'],
            'multipart_chunksize': self.params['multipart_chunksize'],
            'max_concurrency': self.params['max_concurrency'],
            'total_size': total_size,
            'test_time': test_time,
            'errors': error_count,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
        }
        stats.update(self._make_aggr(self.timings))
        return stats
