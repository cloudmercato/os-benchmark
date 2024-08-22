from concurrent.futures import ThreadPoolExecutor
from os_benchmark import utils
from os_benchmark import errors
from . import base


class Benchmark(base.BaseSetupObjectsBenchmark):
    """Time objects downloading"""
    @staticmethod
    def make_parser_args(parser):
        parser.add_argument('--storage-class', required=False)
        parser.add_argument('--bucket-prefix', required=False, type=utils.unescape)
        parser.add_argument('--bucket-suffix', required=False, type=utils.unescape)
        parser.add_argument('--object-size', type=int, required=False)
        parser.add_argument('--object-number', type=int, required=False)
        parser.add_argument('--object-prefix', required=False)
        parser.add_argument('--multipart-threshold', type=int, default=base.MULTIPART_THREHOLD)
        parser.add_argument('--multipart-chunksize', type=int, default=base.MULTIPART_CHUNKSIZE)
        parser.add_argument('--max-concurrency', type=int, default=base.MAX_CONCURRENCY)
        parser.add_argument('--warmup-sleep', type=int, default=0)
        parser.add_argument('--presigned', action="store_true")
        parser.add_argument('--keep-objects', action="store_true")
        parser.add_argument('--bucket-id', default=None)
        parser.add_argument('--parallel-objects', type=int, default=1)

    def run(self, **kwargs):
        def download_objet(url):
            try:
                elapsed = self.timeit(
                    self.driver.download,
                    url=url,
                )[0]
                self.timings.append(elapsed)
            except errors.InvalidHttpCode as err:
                self.errors.append(err)

        def download_objets(urls):
            futures = []
            with ThreadPoolExecutor(max_workers=self.params['parallel_objects']) as executor:
                for url in urls:
                    future = executor.submit(download_objet, url=url)
                    futures.append(future)

            for futures in futures:
                future.result()

        self.sleep(self.params['warmup_sleep'])
        self.total_time = self.timeit(download_objets, urls=self.urls)[0]

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
            'parallel_objects': self.params['parallel_objects'],
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
        stats.update(self._make_aggr(self.timings))
        if error_count:
            error_codes = set([e for e in self.errors])
            stats.update({'error_count_%s' % e.args[1]: 0 for e in self.errors})
            for err in self.errors:
                key = 'error_count_%s' % err.args[1]
                stats[key] += 1
        return stats
