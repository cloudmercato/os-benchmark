import statistics
from os_benchmark import utils
from . import base


class CopyBenchmark(base.BaseSetupObjectsBenchmark):
    """Time objects copy"""
    def setup(self):
        super().setup()
        dst_bucket_name = utils.get_random_name(prefix=self.params.get('bucket_prefix'))
        self.dst_bucket = self.driver.create_bucket(
            name=dst_bucket_name,
            storage_class=self.storage_class,
        )
        self.dst_bucket_id = self.dst_bucket['id']

    def tear_down(self):
        super().tear_down()
        if not self.params.get('keep_objects'):
            self.driver.clean_bucket(bucket_id=self.dst_bucket['id'])

    def run(self, **kwargs):
        self.sleep(self.params['warmup_sleep'])

        def copy_objets(objs):
            for obj in objs:
                try:
                    elapsed = self.timeit(
                        self.driver.copy_object,
                        bucket_id=self.bucket_id,
                        name=obj['name'],
                        dst_bucket_id=self.dst_bucket_id,
                        dst_name=obj['name'],
                    )[0]
                    self.timings.append(elapsed)
                except errors.InvalidHttpCode as err:
                    self.errors.append(err)

        self.total_time = self.timeit(copy_objets, objs=self.objects)[0]

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        test_time = sum(self.timings)
        bw = (total_size/test_time/2**20) if test_time else 0
        rate = (count/test_time) if test_time else 0
        stats = {
            'operation': 'copy',
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
