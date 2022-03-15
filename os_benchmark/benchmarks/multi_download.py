import statistics
import asyncio
try:
    import aiohttp
except ImportError:
    aiohttp = None

from os_benchmark import utils, errors
from os_benchmark.drivers import errors as driver_errors
from . import base


class MultiDownloadBenchmark(base.BaseSetupObjectsBenchmark):
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
        self.sleep(self.params['warmup_sleep'])

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
