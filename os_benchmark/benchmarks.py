"""Benchmark modules"""
import logging
import statistics
from os_benchmark import utils


class BaseBenchmark:
    """Base Benchmark class"""
    def __init__(self, driver):
        self.driver = driver
        self.logger = logging.getLogger('osb.benchmark')
        self.params = {}

    def set_params(self, **kwargs):
        """Set test parameters"""
        self.params.update(kwargs)

    def setup(self):
        """Build benchmark environment"""

    def tear_down(self):
        """Destroy benchmark environment"""

    def run(self, **kwargs):
        """Run benchmark"""
        raise NotImplementedError()

    def make_stats(self):
        """Compute statistics as dict"""
        return {}


class UploadBenchmark(BaseBenchmark):
    """Time objects uploading"""
    def setup(self):
        self.timings = []
        self.objects = []
        bucket_name = utils.get_random_name()
        self.storage_class = self.params.get('storage_class')
        self.logger.debug("Creating bucket '%s'", bucket_name)
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.storage_class,
        )

    def run(self, **kwargs):
        def upload_files():
            for i in range(self.params['object_number']):
                name = utils.get_random_name()
                content = utils.get_random_content(self.params['object_size'])

                self.logger.debug("Uploading object '%s'", name)
                elapsed, obj = utils.timeit(
                    self.driver.upload,
                    bucket_id=self.bucket['id'],
                    storage_class=self.storage_class,
                    name=name,
                    content=content,
                )
                self.timings.append(elapsed)
                self.objects.append(obj)

        self.total_time = utils.timeit(upload_files)[0]

    def tear_down(self):
        self.driver.clean_bucket(bucket_id=self.bucket['id'])

    def make_stats(self):
        count = len(self.timings)
        size = self.params['object_size']
        total_size = count * size
        stats = {
            'operation': 'upload',
            'ops': count,
            'time': self.total_time,
            'rate': (count/self.total_time),
            'bw': (total_size/self.total_time/2**20),
            'object_size': size,
            'total_size': total_size
        }
        if count > 1:
            stats.update({
                'avg': statistics.mean(self.timings),
                'stddev': statistics.stdev(self.timings),
                'med': statistics.median(self.timings),
                'min': min(self.timings),
                'max': max(self.timings),
            })
        return stats


class DownloadBenchmark(BaseBenchmark):
    """Time objects downloading"""

    def setup(self):
        self.timings = []
        self.objects = []
        bucket_name = utils.get_random_name()
        self.logger.debug("Creating bucket '%s'", bucket_name)
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class']
        )
        for i in range(self.params['object_number']):
            name = utils.get_random_name()
            content = utils.get_random_content(self.params['object_size'])

            self.logger.debug("Uploading object '%s'", name)
            obj = self.driver.upload(
                bucket_id=bucket_name,
                storage_class=self.params['storage_class'],
                name=name,
                content=content,
            )
            self.objects.append(obj)
        self.urls = [
            self.driver.get_url(bucket_id=bucket_name, name=obj['name'])
            for obj in self.objects
        ]

    def run(self, **kwargs):
        def download_objets(urls):
            for url in urls:
                self.driver.download(url)
                elapsed = utils.timeit(
                    self.driver.download,
                    url=url,
                )[0]
                self.timings.append(elapsed)

        self.total_time = utils.timeit(download_objets, urls=self.urls)[0]

    def tear_down(self):
        self.driver.clean_bucket(bucket_id=self.bucket['id'])

    def make_stats(self):
        count = len(self.timings)
        size = self.params['object_size']
        total_size = count * size
        stats = {
            'operation': 'download',
            'ops': count,
            'time': self.total_time,
            'rate': (count/self.total_time),
            'bw': (total_size/self.total_time/2**20),
            'object_size': size,
            'total_size': total_size
        }
        if count > 1:
            stats.update({
                'avg': statistics.mean(self.timings),
                'stddev': statistics.stdev(self.timings),
                'med': statistics.median(self.timings),
                'min': min(self.timings),
                'max': max(self.timings),
            })
        return stats
