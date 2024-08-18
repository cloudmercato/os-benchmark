import concurrent
import requests
from os_benchmark import utils
from . import base, errors


def _download(session, url, b_range):
    headers = {'Range': 'bytes=%s-%s' % b_range}
    try:
        session.get(url, headers=headers)
    except requests.exceptions.ChunkedEncodingError as err:
        raise errors.ConnectionError(err)


class MultiDownloadBenchmark(base.BaseSetupObjectsBenchmark):
    """Time objects downloading using multi-range"""
    @staticmethod
    def make_parser_args(parser):
        parser.add_argument('--storage-class', required=False)
        parser.add_argument('--bucket-prefix', required=False, type=utils.unescape)
        parser.add_argument('--bucket-suffix', required=False, type=utils.unescape)
        parser.add_argument('--object-size', type=int, required=False)
        parser.add_argument('--object-number', type=int, required=False)
        parser.add_argument('--object-prefix', required=False)
        parser.add_argument('--presigned', action="store_true")
        parser.add_argument('--warmup-sleep', type=int, default=0)
        parser.add_argument('--multipart-chunksize', type=int, default=base.MULTIPART_CHUNKSIZE)
        parser.add_argument('--process-number', type=int, default=base.MAX_CONCURRENCY)
        parser.add_argument('--max-concurrency', type=int, default=base.MAX_CONCURRENCY)
        parser.add_argument('--upload-multipart-threshold', type=int, default=base.MULTIPART_THREHOLD)
        parser.add_argument('--upload-multipart-chunksize', type=int, default=base.MULTIPART_CHUNKSIZE)
        parser.add_argument('--upload-max-concurrency', type=int, default=base.MAX_CONCURRENCY)
        parser.add_argument('--keep-objects', action="store_true")
        parser.add_argument('--bucket-id', default=None)

    def setup(self):
        if self.params.get('multipart_chunksize'):
            self.multipart_chunksize = self.params.get('multipart_chunksize')
        elif self.params['object_size'] < (64*2**20):  # 64MB
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
                futures.append(pool.submit(
                    _download,
                    self.session,
                    url,
                    b_range,
                ))
            for future in futures:
                while not future.done():
                    if future.exception():
                        self.errors.append(future.exception())
                        break
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
        stats = {
            'operation': 'multi_download',
            'ops': count,
            'time': self.total_time,
            'bucket_prefix': self.params.get('bucket_prefix'),
            'object_size': size,
            'object_number': self.params['object_number'],
            'object_prefix': self.params.get('object_prefix'),
            'multipart_chunksize': self.multipart_chunksize,
            'process_number': self.params['process_number'],
            'chunk_number': self.chunk_number,
            'total_size': total_size,
            'total_time': self.total_time,
            'test_time': test_time,
            'errors': error_count,
            'driver': self.driver.id,
            'read_timeout': self.driver.read_timeout,
            'connect_timeout': self.driver.connect_timeout,
            'presigned': int(self.params['presigned']),
            'warmup_sleep': self.params['warmup_sleep'],
            'error_timeout': 0,
        }
        stats.update(self._make_aggr(self.timings, 'time'))
        bws = [(size/t) for t in self.timings]
        stats.update(self._make_aggr(bws, 'bw'))

        if error_count:
            stats.update({'error_count_%s' % e.args[1]: 0 for e in self.errors})
            for err in self.errors:
                key = 'error_count_%s' % err.args[1]
                stats[key] += 1
        return stats
