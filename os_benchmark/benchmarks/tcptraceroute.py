try:
    import scapy.all as scapy
except ImportError:
    pass
from . import base


class TcpTracerouteBenchmark(base.BaseNetworkBenchmark):
    """TcpTraceroute endpoint"""
    def _traceroute(self, ip, port):
        replies = []
        for ttl in range(self.params['max_ttl']):
            packet = scapy.IP(dst=ip, ttl=ttl) / scapy.TCP(dport=port, flags="S")
            results, unanswered = scapy.sr(
                packet,
                timeout=self.params['timeout'],
                verbose=self.params['scapy_verbose'],
            )
            replies.append((results, unanswered))
            if results and results[0].answer.src == ip:
                break
        return replies

    def run(self, **kwargs):
        self.sleep(self.params['warmup_sleep'])
        
        def traceroute():
            for i in range(self.params['count']):
                elapsed, reply = self.timeit(self._traceroute, self.ip, self.port)
                if reply:
                    self.timings.append(elapsed)
                    self.replies.append(reply)
                else:
                    self.errors.append(TimeoutError())

        self.total_time = self.timeit(traceroute)[0]

    def make_stats(self):
        count = self.params['count']
        error_count = len(self.errors)
        size = self.params['object_size']
        total_size = count * size
        test_time = sum(self.timings)
        bw = (total_size/test_time/2**20) if test_time else 0
        stats = {
            'operation': 'tcptraceroute',
            'ops': count,
            'time': self.total_time,
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
            'port': self.port,
            'timeout': self.params['timeout'],
            'max_ttl': self.params['max_ttl'],
        }

        hops = []
        for reply in self.replies:
            hops.append(len(reply))
        stats.update(self._make_aggr(hops, 'hops'))
        return stats
