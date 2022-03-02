"""Benchmark modules"""
import logging
import re
import statistics
import subprocess
import time

import asyncio
from concurrent.futures._base import TimeoutError as AsyncTimeoutError
try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    from pycurlb import Curler
except ImportError:
    pass

from os_benchmark import utils, errors
from os_benchmark.drivers import errors as driver_errors


if aiohttp is not None:
    ASYNC_TIMEOUT_ERRORS = (
        asyncio.TimeoutError,
        AsyncTimeoutError,
        aiohttp.client_exceptions.ServerTimeoutError
    )

class BenchmarkError(Exception):
    pass


class BaseBenchmark:
    """Base Benchmark class"""
    def __init__(self, driver):
        self.driver = driver
        self.logger = logging.getLogger('osb')
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


class BaseSetupObjectsBenchmark(BaseBenchmark):
    def setup(self):
        self.timings = []
        self.errors = []
        self.objects = []
        bucket_name = utils.get_random_name(prefix=self.params.get('bucket_prefix'))
        self.logger.debug("Creating bucket '%s'", bucket_name)
        self.storage_class = self.params.get('storage_class')
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.storage_class,
        )
        self.bucket_id = self.bucket['id']
        for i in range(self.params['object_number']):
            name = utils.get_random_name(prefix=self.params.get('object_prefix'))
            content = utils.get_random_content(self.params['object_size'])

            self.logger.debug("Uploading object '%s'", name)
            try:
                obj = self.driver.upload(
                    bucket_id=self.bucket_id,
                    storage_class=self.storage_class,
                    name=name,
                    content=content,
                )
            except driver_errors.DriverError as err:
                self.logger.warning("Error during file uploading, tearing down the environment.")
                self.tear_down()
                raise
            self.objects.append(obj)
        self.urls = [
            self.driver.get_url(
                bucket_id=self.bucket_id,
                name=obj['name'],
                bucket_name=self.bucket.get('name', self.bucket_id),
                presigned=self.params['presigned']
            )
            for obj in self.objects
        ]
        time.sleep(self.params['warmup_sleep'])

    def tear_down(self):
        self.driver.clean_bucket(bucket_id=self.bucket['id'])


class UploadBenchmark(BaseBenchmark):
    """Time objects uploading"""
    def setup(self):
        self.timings = []
        self.objects = []
        self.errors = []
        self.driver.setup(**self.params)

        bucket_name = utils.get_random_name(prefix=self.params.get('bucket_prefix'))
        self.storage_class = self.params.get('storage_class')
        self.logger.debug("Creating bucket '%s'", bucket_name)
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.storage_class,
        )

    def run(self, **kwargs):
        def upload_files():
            for i in range(self.params['object_number']):
                name = utils.get_random_name(prefix=self.params.get('object_prefix'))
                content = utils.get_random_content(self.params['object_size'])

                self.logger.debug("Uploading object '%s'", name)
                try:
                    elapsed, obj = utils.timeit(
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


        self.total_time = utils.timeit(upload_files)[0]

    def tear_down(self):
        self.driver.clean_bucket(bucket_id=self.bucket['id'])

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
        if count > 1:
            stats.update({
                'avg': statistics.mean(self.timings),
                'stddev': statistics.stdev(self.timings),
                'med': statistics.median(self.timings),
                'min': min(self.timings),
                'max': max(self.timings),
            })
        return stats


class DownloadBenchmark(BaseSetupObjectsBenchmark):
    """Time objects downloading"""
    def run(self, **kwargs):
        def download_objets(urls):
            for url in urls:
                try:
                    elapsed = utils.timeit(
                        self.driver.download,
                        url=url,
                    )[0]
                    self.timings.append(elapsed)
                except errors.InvalidHttpCode as err:
                    self.errors.append(err)

        self.total_time = utils.timeit(download_objets, urls=self.urls)[0]

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        test_time = sum(self.timings)
        bw = (total_size/test_time/2**20) if test_time else 0
        rate = (count/test_time) if test_time else 0
        stats = {
            'operation': 'download',
            'ops': count,
            'time': self.total_time,
            'bw': bw,
            'rate': rate,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'max_concurrency': 1,
            'multipart_threshold': 0,
            'multipart_chunksize': 0,
            'total_size': total_size,
            'test_time': test_time,
            'errors': error_count,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
            'presigned': int(self.params['presigned']),
            'warmup_sleep': self.params['warmup_sleep'],
        }
        if count > 1:
            stats.update({
                'avg': statistics.mean(self.timings),
                'stddev': statistics.stdev(self.timings),
                'med': statistics.median(self.timings),
                'min': min(self.timings),
                'max': max(self.timings),
            })
        if error_count:
            error_codes = set([e for e in self.errors])
            stats.update({'error_count_%s' % e.args[1]: 0 for e in self.errors})
            for err in self.errors:
                key = 'error_count_%s' % err.args[1]
                stats[key] += 1
        return stats


class MultiDownloadBenchmark(BaseSetupObjectsBenchmark):
    """Time objects downloading using multi-range"""
    def setup(self):
        if self.params.get('multipart_chunksize'):
            self.multipart_chunksize = self.params.get('multipart_chunksize')
        elif self.params['object_size'] < (64*2**20): # 64MB
            self.multipart_chunksize = self.params['object_size']
        else:
            self.multipart_chunksize = 64*2**20
        self.chunk_number = self.params['object_size'] // self.multipart_chunksize
        super().setup()

    def run(self, **kwargs):

        async def download(session, url, b_range):
            headers = {'Range': 'bytes=%s-%s' % b_range}
            async with session.get(url, headers=headers) as response:
                elapsed, _ = await utils.async_timeit(response.text)
                return elapsed, response

        async def download_objets():
            connector = aiohttp.TCPConnector(
                limit=self.params.get('max_concurrency'),
            )
            session_kwargs['connector'] = connector
            session = aiohttp.ClientSession(**session_kwargs)
            requests = []
            for url in self.urls:
                for i in range(self.chunk_number):
                    b_range = (i*self.multipart_chunksize, min(i*self.multipart_chunksize+self.multipart_chunksize, self.params['object_size']))
                    requests.append(download(session, url, b_range))
            results = await asyncio.gather(*requests)
            await session.close()
            connector.close()

            for elapsed, response in results:
                self.timings.append(elapsed)

        session_kwargs = {
            'raise_for_status': True,
            'read_timeout': self.driver.read_timeout,
            'conn_timeout': self.driver.connect_timeout
        }
        def run():
            asyncio.run(download_objets())
        self.total_time = utils.timeit(run)[0]

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        test_time = sum(self.timings)
        bw = (total_size/test_time/2**20) if test_time else 0
        rate = (count/test_time) if test_time else 0
        stats = {
            'operation': 'download',
            'ops': count,
            'time': self.total_time,
            'bw': bw,
            'rate': rate,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'max_concurrency': self.params.get('max_concurrency'),
            'multipart_threshold': 0,
            'multipart_chunksize': self.multipart_chunksize,
            'chunk_number': self.chunk_number,
            'total_size': total_size,
            'test_time': test_time,
            'errors': error_count,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
            'presigned': int(self.params['presigned']),
            'warmup_sleep': self.params['warmup_sleep'],
        }
        if count > 1:
            stats.update({
                'avg': statistics.mean(self.timings),
                'stddev': statistics.stdev(self.timings),
                'med': statistics.median(self.timings),
                'min': min(self.timings),
                'max': max(self.timings),
            })
        if error_count:
            error_codes = set([e for e in self.errors])
            stats.update({'error_count_%s' % e.args[1]: 0 for e in self.errors})
            for err in self.errors:
                key = 'error_count_%s' % err.args[1]
                stats[key] += 1
        return stats


class AbBenchmark(DownloadBenchmark):
    """Measure objects downloading with Apache Benchmark"""
    result_fields = {
        'SSL/TLS Protocol': 'tls_protocol',
        'Time taken for tests': 'test_time',
        'Complete requests': 'complete_requests',
        'Failed requests': 'failed_requests',
        'Non-2xx responses': 'non_200_requests',
        'Total transferred': 'total_transfered',
        'HTML transferred': 'transfered',
        'Requests per second': 'request_rate',
        'Transfer rate': 'byte_rate',
        'Connect': 'connect_time',
        'Processing': 'processing_time',
        'Waiting': 'waiting_time',
        'Total': 'total_time',
    }
    stat_fields = ('min', 'mean', 'stddev', 'median', 'max')
    re_digit = re.compile('\s*([\d.]+)\s*')

    def parse_ab(self, stdout):
        raw_data = dict([
            line.split(':', 1)
            for line in stdout.splitlines()
            if ':' in line
        ])
        raw_data = {
            self.result_fields[k]: v.strip()
            for k, v in raw_data.items()
            if k in self.result_fields
        }
        data = raw_data.copy()

        def parse_stat(key, line):
            splitted = line.split()
            return {
                ('%s_%s' % (key, suf)): splitted[i].strip()
                for i, suf in enumerate(self.stat_fields)
            }

        for key, line in raw_data.items():
            if key in ('test_time', 'transfered', 'request_rate', 'total_transfered'):
                data[key] = self.re_digit.findall(line)[0]
            elif key in ('complete_requests', 'failed_requests', 'non_200_requests'):
                data[key] = line.strip()
            elif key in ('connect_time', 'processing_time', 'waiting_time', 'total_time'):
                line = data.pop(key)
                data.update(parse_stat(key, line))
        return data


    def run_ab(self, url):
        cmd = 'ab -c %(concurrency)d -t %(timelimit)d -n %(num_requests)s' % self.params
        if self.params['keep_alive']:
            cmd += ' -k'
        if self.params['source_address']:
            cmd += ' -B %s' % self.params['source_address']
        cmd += ' %s' % url
        self.logger.debug('Run "%s"', cmd)
        try:
            out = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = out.communicate()
        except FileNotFoundError as err:
            self.logger.warning("Error during ab launching: %s", err)
            raise BenchmarkError(str(err))
        return self.parse_ab(stdout.decode())

    def run(self, **kwargs):
        def download_objets(urls):
            for url in urls:
                try:
                    output = self.run_ab(url=url)
                    self.timings.append(output)
                except errors.InvalidHttpCode as err:
                    self.errors.append(err)

        self.total_time = utils.timeit(download_objets, urls=self.urls)[0]

    def tear_down(self):
        self.driver.clean_bucket(bucket_id=self.bucket['id'])

    def make_stats(self):
        stats = {
            'operation': 'ab',
            'keep_alive': int(self.params['keep_alive']),
            'source_address': self.params['source_address'],
            'concurrency': self.params['concurrency'],
            'timelimit': self.params['timelimit'],
            'num_requests': self.params['num_requests'],
            'time': self.total_time,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': self.params['object_size'],
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'total_size': self.params['object_size'],
            'driver': self.driver.id,
            'presigned': int(self.params['presigned']),
        }
        for field in self.timings[0]:
            values = [float(r[field]) for r in self.timings if r[field].isdecimal()]
            if values:
                stats[field] = statistics.mean(values)
        return stats


class PycurlbBenchmark(BaseSetupObjectsBenchmark):
    """Time request with pycurlb"""
    timing_fields = (
        'namelookup_time',
        'connect_time',
        'appconnect_time',
        'pretransfer_time',
        'starttransfer_time',
        'total_time',
        'speed_download',
    )
    math_func = {
        'avg': statistics.mean,
        'stddev': statistics.stdev,
        'med': statistics.median,
        'min': min,
        'max': max,
    }

    def run(self, **kwargs):
        curler = Curler()
        for url in self.urls:
            try:
                info = curler.perform(
                    url=url,
                    connect_timeout=self.driver.connect_timeout,
                    accept_timeout_ms=self.driver.read_timeout*1000,
                    forbid_reuse=int(not self.params['keep_alive']),
                )
                self.timings.append(info)
            except errors.InvalidHttpCode as err:
                self.errors.append(err)

    def _make_math(self, values, func_name):
        func = self.math_func[func_name]
        try:
            return func(values)
        except statistics.StatisticsError:
            if func_name == 'stddev':
                return .0
            return values[0]

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        stats = {
            'operation': 'curl',
            'ops': count,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'total_size': total_size,
            'errors': error_count,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
            'presigned': int(self.params['presigned']),
            'warmup_sleep': self.params['warmup_sleep'],
            'keep_alive': int(self.params['keep_alive']),
        }

        for field in self.timing_fields:
            values = [t[field] for t in self.timings]
            for func in self.math_func:
                key = '%s_%s' % (field, func)
                stats[key] = self._make_math(values, func)

        if error_count:
            error_codes = set([e for e in self.errors])
            stats.update({'error_count_%s' % e.args[1]: 0 for e in self.errors})
            for err in self.errors:
                key = 'error_count_%s' % err.args[1]
                stats[key] += 1
        return stats


class VideoStreamingBenchmark(BaseSetupObjectsBenchmark):
    """Time request with pycurlb"""
    def run(self, **kwargs):

        async def download(session, url):
            try:
                async with session.get(url) as response:
                    elapsed, _ = await utils.async_timeit(response.read)
            except aiohttp.client_exceptions.ClientResponseError as err:
                return -1, err
            except aiohttp.client_exceptions.ClientConnectorError as err:
                return -1, err
            except ASYNC_TIMEOUT_ERRORS as err:
                return -1, err
            return elapsed, response

        async def download_objets(delay=0):
            await asyncio.sleep(delay)
            connector = aiohttp.TCPConnector(
                limit=self.params.get('max_concurrency'),
            )
            session_kwargs['connector'] = connector
            session = aiohttp.ClientSession(**session_kwargs)
            timings, errors = [], []
            for url in self.urls:
                # self.logger.warning("Download %s", url)
                await asyncio.sleep(self.params['sleep_time'])
                elapsed, response = await download(session, url)
                if elapsed == -1:
                    errors.append(response)
                else:
                    timings.append(elapsed)
            await session.close()
            connector.close()
            return timings, errors

        session_kwargs = {
            'raise_for_status': True,
            'read_timeout': self.driver.read_timeout,
            'conn_timeout': self.driver.connect_timeout
        }
        self.timings = []
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)

        def run():
            clients = []
            for i in range(self.params['client_number']):
                # self.logger.warning("Start client #%d", i)
                delay = i * self.params['delay_time']
                clients.append(download_objets(i))
            tasks = asyncio.gather(*clients)
            results = loop.run_until_complete(tasks)
            for timings, errors in results:
                self.timings.extend(timings)
                self.errors.extend(errors)

        self.total_time = utils.timeit(run)[0]
        loop.close()

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        bws = [(size/t) for t in self.timings]
        stats = {
            'operation': 'video_streaming',
            'ops': count + error_count,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'total_size': total_size,
            'total_time': self.total_time,
            'errors': count,
            'errors': error_count,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
            'presigned': int(self.params['presigned']),
            'warmup_sleep': self.params['warmup_sleep'],
            'sleep_time': int(self.params['sleep_time']),
            'client_number': int(self.params['client_number']),
            'delay_time': int(self.params['delay_time']),
        }

        if count > 1:
            stats.update({
                'time_avg': statistics.mean(self.timings),
                'time_stddev': statistics.stdev(self.timings),
                'time_med': statistics.median(self.timings),
                'time_min': min(self.timings),
                'time_max': max(self.timings),
                'bw_avg': statistics.mean(bws),
                'bw_stddev': statistics.stdev(bws),
                'bw_med': statistics.median(bws),
                'bw_min': min(bws),
                'bw_max': max(bws),
            })

        if error_count:
            for err in self.errors:
                if isinstance(err, aiohttp.client_exceptions.ClientConnectorError):
                    key = 'error_client'
                elif isinstance(err, ASYNC_TIMEOUT_ERRORS):
                    key = 'error_timeout'
                else:
                    key = 'error_count_%s' % err.args[1]
                stats.setdefault(key, 0)
                stats[key] += 1
        return stats
