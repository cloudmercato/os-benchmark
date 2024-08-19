import importlib
import os
import logging
import time
import socket
from urllib.parse import urlparse
import statistics

import asyncio
from concurrent.futures._base import TimeoutError as AsyncTimeoutError
from concurrent.futures import ThreadPoolExecutor
try:
    import aiohttp
except ImportError:
    aiohttp = None
try:
    from probes import ProbeManager
    has_probes = True
except ImportError:
    has_probes = False

from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors


MULTIPART_THREHOLD = 64 * 2**20
MULTIPART_CHUNKSIZE = 8 * 2**20
MAX_CONCURRENCY = os.cpu_count() * 2

if aiohttp is not None:
    ASYNC_TIMEOUT_ERRORS = (
        asyncio.TimeoutError,
        AsyncTimeoutError,
        aiohttp.client_exceptions.ServerTimeoutError
    )

DEFAULT_PORTS = {
    'http': 80,
    'https': 443,
}

AGGR_FUNCS = {
    'avg': statistics.mean,
    'stddev': statistics.stdev,
    'med': statistics.median,
    'min': min,
    'max': max,
    'perc95': utils.percentile95
}


class BenchmarkError(Exception):
    """General benchmark error"""


class BaseBenchmark:
    """Base Benchmark class"""
    def __init__(self, driver):
        self.driver = driver
        self.logger = logging.getLogger('osb')
        self.params = {}

    def set_params(self, **kwargs):
        """Set test parameters"""
        self.params.update(kwargs)

    def sleep(self, delay):
        """Shortcut for time.sleep"""
        time.sleep(delay)

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

    def _make_aggr(self, values, name=None, decimals=None):
        stats = {}
        if not values:
            return stats

        if len(values) == 1:  # Lazy compute
            values *= 2
        for func_name, func in AGGR_FUNCS.items():
            key = '%s_%s' % (name, func_name) if name else func_name
            value = func(values)
            if decimals == 0:
                value = int(value)
            elif decimals is not None and decimals > 0:
                value = round(value, decimals)
            stats[key] = value
        return stats

    def timeit(self, *args, **kwargs):
        return utils.timeit(*args, **kwargs)

    def start_monitoring(self, probers, interval=5):
        if not probers:
            probers = [
                'probes.probers.system.CpuProber',
                'probes.probers.system.MemoryProber',
                'probes.probers.system.NetworkProber',
            ]
        self.probe_manager = ProbeManager(
            interval=interval,
            probers=probers,
        )
        self.probe_manager.start()

    def stop_monitoring(self):
        self.probe_manager.stop()

    def get_monitoring_results(self):
        return self.probe_manager.get_results()


class BaseSetupObjectsBenchmark(BaseBenchmark):
    def _create_bucket(self, name=None):
        bucket_name = name or utils.get_random_name(
            size=self.params.get('bucket_name_size', 30),
            prefix=self.params.get('bucket_prefix'),
            suffix=self.params.get('bucket_suffix'),
        )

        self.logger.info("Creating bucket '%s'", bucket_name)
        self.storage_class = self.params.get('storage_class')
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.storage_class,
        )
        self.bucket_id = self.bucket['id']

        max_workers = self.params.get('parallel_objects') or min(2, max(os.cpu_count(), 64))
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(self.params['object_number']):
                future = executor.submit(self._make_upload)
                futures.append(future)

        exceptions = [f.exception() for f in futures if f.exception()]
        if exceptions:
            raise exceptions[0]

    def _reuse_bucket(self):
        self.bucket_id = self.params['bucket_id']
        self.logger.debug("Reuse bucket '%s'", self.bucket_id)
        self.bucket = self.driver.get_bucket(bucket_id=self.bucket_id)
        self.storage_class = self.params.get('storage_class') or \
            self.bucket.get('storage_class')

        self.objects = self.driver.list_objects(bucket_id=self.bucket_id)
        for obj_name in self.objects:
            self.urls.append(self.driver.get_url(
                bucket_id=self.bucket_id,
                name=obj_name,
                bucket_name=self.bucket.get('name', self.bucket_id),
                presigned=self.params['presigned']
            ))

    def _make_upload(self):
        name = utils.get_random_name(prefix=self.params.get('object_prefix'))
        content = utils.get_random_content(self.params['object_size'])

        self.logger.debug("Uploading object '%s'", name)
        multipart_chunksize = self.params.get('upload_multipart_chunksize') or \
            self.params.get('multipart_chunksize', 128*2**20)
        multipart_threshold = self.params.get('upload_multipart_threshold') or \
            self.params.get('multipart_threshold', 128*2**20)
        max_concurrency = self.params.get('upload_max_concurrency') or \
            self.params.get('max_concurrency')
        try:
            obj = self.driver.upload(
                bucket_id=self.bucket_id,
                storage_class=self.storage_class,
                name=name,
                content=content,
                multipart_chunksize=multipart_chunksize,
                multipart_threshold=multipart_threshold,
                max_concurrency=max_concurrency,
            )
        except driver_errors.DriverError as err:
            self.logger.warning("Error during file uploading, tearing down the environment: %s", err)
            raise
        self.objects.append(obj)
        self.urls.append(self.driver.get_url(
            bucket_id=self.bucket_id,
            name=obj['name'],
            bucket_name=self.bucket.get('name', self.bucket_id),
            presigned=self.params['presigned']
        ))

    def setup(self):
        self.logger.debug("Bench params '%s'", self.params)
        self.timings = []
        self.errors = []
        self.objects = []

        self.urls = []

        # Re-use bucket
        if self.params.get('bucket_id'):
            try:
                self._reuse_bucket()
                if self.objects:
                    return
                self.logger.warning("Bucket %s is empty", self.bucket_id)
            except driver_errors.DriverBucketUnfoundError:
                self.logger.warning("Bucket %s not found", self.bucket_id)
        # Or create
        self._create_bucket(name=self.params.get('bucket_id'))

    def tear_down(self):
        if not self.params.get('keep_objects'):
            try:
                self.driver.clean_bucket(bucket_id=self.bucket['id'])
            except driver_errors.DriverNonEmptyBucketError as err:
                self.logger.error(err)


class BaseNetworkBenchmark(BaseSetupObjectsBenchmark):
    def setup(self):
        super().setup()
        self.obj = self.objects[0]
        url = self.driver.get_url(
            bucket_id=self.bucket_id,
            name=self.obj['name'],
            bucket_name=self.bucket.get('name', self.bucket_id),
        )
        self.parsed_url = urlparse(url)
        self.port = self.parsed_url.port
        if not self.port:
            self.port = DEFAULT_PORTS[self.parsed_url.scheme]
        self.addr_info = socket.getaddrinfo(self.parsed_url.hostname, self.port)
        self.ip = self.addr_info[0][-1][0]

        self.replies = []


def get_benchmark(key):
    """Get driver from its key"""
    module_path = 'os_benchmark.benchmarks.%s' % key
    class_name = 'Benchmark'
    module = importlib.import_module(module_path)
    driver_class = getattr(module, class_name)
    return driver_class
