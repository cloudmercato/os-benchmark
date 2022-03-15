import socket
from urllib.parse import urlparse
import scapy.all as scapy
import statistics
try:
    import scapy.all as scapy
except ImportError:
    pass
from os_benchmark import utils, errors
from os_benchmark.drivers import errors as driver_errors
from . import base


class PingBenchmark(base.BaseSetupObjectsBenchmark):
    """Time ping endpoint"""
    def _ping(self, ip):
        packet = scapy.IP(ip.encode(), ttl=self.params['ttl']) / scapy.ICMP()
        reply = scapy.sr1(
            packet,
            timeout=self.params['timeout'],
            verbose=self.params['scapy_verbose'],
        )

    def run(self, **kwargs):
        self.sleep(self.params['warmup_sleep'])
        obj = self.objects[0]
        url = self.driver.get_url(
            bucket_id=self.bucket_id,
            name=obj['name'],
            bucket_name=self.bucket.get('name', self.bucket_id),
        )
        self.parsed_url = urlparse(url)
        addr_info = socket.getaddrinfo(self.parsed_url.hostname, self.parsed_url.port)
        self.ip = addr_info[0][-1][0]
        
        def ping():
            for i in range(self.params['count']):
                elapsed, reply = utils.timeit(self._ping, self.ip)
                if reply:
                    self.timings.append(elapsed)
                else:
                    self.errors.append(TimeoutError())

        self.total_time = utils.timeit(ping)[0]

    def make_stats(self):
        count = len(self.timings)
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        test_time = sum(self.timings)
        bw = (total_size/test_time/2**20) if test_time else 0
        rate = (count/test_time) if test_time else 0
        stats = {
            'operation': 'ping',
            'ops': count,
            'time': self.total_time,
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
            'hostname': self.parsed_url.hostname,
            'ip': self.ip,
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
