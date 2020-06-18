import swiftclient
from os_benchmark.drivers import base


class Driver(base.BaseDriver):
    @property
    def swift(self):
        if not hasattr(self, '_swift'):
            self._swift = swiftclient.Connection(
                **self.kwargs
            )
        return self._swift

    def list_buckets(self, **kwargs):
        _, raw_buckets = self.swift.get_account()
        buckets = [
            {'name': b['name']}
            for b in raw_buckets
        ]
        return buckets
