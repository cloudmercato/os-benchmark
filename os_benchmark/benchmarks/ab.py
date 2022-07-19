import re
import subprocess
from os_benchmark import errors
from . import base


class AbBenchmark(base.BaseSetupObjectsBenchmark):
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
            if key in ('test_time', 'transfered', 'request_rate', 'total_transfered', 'byte_rate'):
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
            raise base.BenchmarkError(str(err))
        return self.parse_ab(stdout.decode())

    def run(self, **kwargs):
        def download_objets(urls):
            for url in urls:
                try:
                    output = self.run_ab(url=url)
                    self.timings.append(output)
                except errors.InvalidHttpCode as err:
                    self.errors.append(err)

        self.sleep(self.params['warmup_sleep'])
        self.total_time = self.timeit(download_objets, urls=self.urls)[0]

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
            values = [
                float(r[field]) for r in self.timings
                if r[field].replace('.', '').isdecimal()
            ]
            stats.update(self._make_aggr(values, field))
        return stats
