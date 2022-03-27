from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from os_benchmark.benchmarks import base


class GetObjectTorrentTest(base.BaseBenchmark):
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
        torrent = self.driver.get_object_torrent(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
        )
        self.logger.debug('Torrent magnet: %s', torrent)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])
