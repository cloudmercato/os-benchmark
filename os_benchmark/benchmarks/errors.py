from os_benchmark import errors as base


class BenchmarkError(base.OsbError):
    """Base Benchmark error"""


class ConnectionError(BenchmarkError):
    pass
