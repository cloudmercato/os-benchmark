import statistics
try:
    from pycurlb import Curler
except ImportError:
    pass
from os_benchmark import utils, errors
from os_benchmark.drivers import errors as driver_errors
from . import base


class PycurlbBenchmark(base.BaseSetupObjectsBenchmark):
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
        self.sleep(self.params['warmup_sleep'])
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
