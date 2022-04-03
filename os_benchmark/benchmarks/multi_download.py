import concurrent
import requests
from os_benchmark import utils
from . import base


def _download(session, url, b_range):
    headers = {'Range': 'bytes=%s-%s' % b_range}
    response = session.get(url, headers=headers)
    response.text


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
        self.session = requests.Session()
        super().setup()

    def run(self, **kwargs):
        self.sleep(self.params['warmup_sleep'])
        pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.params['process_number'],
        )

        def download_object(url):
            futures = []
            for i in range(self.chunk_number):
                b_range = (i*self.multipart_chunksize, min(i*self.multipart_chunksize+self.multipart_chunksize, self.params['object_size']-1))
                headers = {'Range': 'bytes=%s-%s' % b_range}
                futures.append(pool.submit(
                    _download,
                    self.session,
                    url,
                    b_range,
                ))
            for future in futures:
                while not future.done():
                    future.result()

        def run():
            for url in self.urls:
                elapsed = self.timeit(download_object, url)[0]
                self.timings.append(elapsed)

        self.total_time = utils.timeit(run)[0]
        pool.shutdown()

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
        stats.update(self._make_aggr(self.timings))
        if error_count:
            error_codes = set([e for e in self.errors])
            stats.update({'error_count_%s' % e.args[1]: 0 for e in self.errors})
            for err in self.errors:
                key = 'error_count_%s' % err.args[1]
                stats[key] += 1
        return stats
