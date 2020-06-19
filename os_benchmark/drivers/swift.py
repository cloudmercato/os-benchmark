import swiftclient
from os_benchmark.drivers import base, errors


class Driver(base.RequestsMixin, base.BaseDriver):
    default_kwargs = {}

    @property
    def swift(self):
        if not hasattr(self, '_swift'):
            kwargs = self.kwargs
            kwargs.update(self.default_kwargs)
            self._swift = swiftclient.Connection(**self.kwargs)
        return self._swift

    def list_buckets(self, **kwargs):
        _, raw_buckets = self.swift.get_account()
        buckets = [
            {'id': b['name']}
            for b in raw_buckets
        ]
        return buckets

    def create_bucket(self, name, acl='public-read', **kwargs):
        headers = {}
        if acl == 'public-read':
            headers['X-Container-Read'] = '.r:*'
        self.swift.put_container(name, headers=headers)
        return {'id': name}

    def delete_bucket(self, bucket_id, **kwargs):
        try:
            self.swift.delete_container(bucket_id)
        except swiftclient.ClientException as err:
            if err.http_status == 404:
                return
            if err.http_status == 409:
                raise errors.DriverNonEmptyBucketError(err.args[0])

    def list_objects(self, bucket_id, **kwargs):
        try:
            _, objs = self.swift.get_container(bucket_id)
        except swiftclient.ClientException as err:
            if err.http_status == 404:
                raise errors.DriverBucketUnfoundError(err.args[0])
            raise
        return [o['name'] for o in objs]

    def upload(self, bucket_id, name, content, acl='public-read', **kwargs):
        self.swift.put_object(bucket_id, name, content)
        return {'name': name}

    def delete_object(self, bucket_id, name, **kwargs):
        try:
            self.swift.delete_object(bucket_id, name)
        except swiftclient.ClientException as err:
            if err.http_status == 404:
                return
            raise
