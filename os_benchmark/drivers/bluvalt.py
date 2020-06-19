from os_benchmark.drivers import swift


class Driver(swift.Driver):
    """Bluvalt Swift Driver"""
    default_kwargs = {
        'authurl': 'https://api-object.bluvalt.com:8083/auth/v1.0',
    }

    @property
    def session_headers(self):
        if not hasattr(self, '_session_headers'):
            self.base_url, self.token = self.swift.get_auth()
            self._session_headers = {'X-Auth-Token': self.token}
        return self._session_headers

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.swift.url, bucket_id, name)
        return url
