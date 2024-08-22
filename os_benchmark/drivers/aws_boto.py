from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """AWS S3 Boto Driver"""
    id = 'aws_s3'
    old_acl = False
    default_object_acl = 'public-read'

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.kwargs['endpoint_url'], bucket_id, name)
        return url
