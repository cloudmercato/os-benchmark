from os_benchmark.drivers import swift


class Driver(swift.Driver):
    """Hopla.cloud Swift Driver"""
    id = 'hopla'
    swift_endpoint = 'https://s3-fr-east-1.pub.hopla.cloud/swift/v1/'
    default_kwargs = {
        'authurl': 'https://fr-east-1.pub.hopla.cloud:13000/v3',
        'auth_version': 3,
        'os_options': {
            'region_name': 'fr-east-1',
            'project_domain_name': 'Default',
            'user_domain_name': 'Default',
            'project_name': 'Default',
        }
    }

    def get_url(self, bucket_id, name, **kwargs):
        url = '%sAUTH_%s/%s/%s' % (
            self.swift_endpoint,
            self.kwargs['os_options']['project_id'],
            bucket_id,
            name
        )
        return url
