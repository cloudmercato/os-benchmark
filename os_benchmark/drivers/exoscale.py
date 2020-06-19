"""
.. warning
  This driver requires boto3.
"""
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Exoscale S3 Driver"""
    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
