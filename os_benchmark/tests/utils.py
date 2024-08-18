from os_benchmark.drivers import base


class InMemoryDriver(base.BaseDriver):
    id = "in-memory"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buckets = []
        self.objects = {}

    def list_buckets(self, **kwargs):
        return [{'name': n, 'id': n} for n in self.buckets]

    def create_bucket(self, name, **kwargs):
        self.buckets.append(name)
        self.objects[name] = {}
        return {'name': name, 'id': name}

    def delete_bucket(self, bucket_id, **kwargs):
        if bucket_id in self.buckets:
            try:
                idx = self.buckets.index(bucket_id)
                del self.buckets[idx]
            except ValueError:
                pass
        if bucket_id in self.objects:
            del self.objects[bucket_id]

    def list_objects(self, bucket_id, **kwargs):
        if bucket_id in self.objects:
            return [o for o in self.objects[bucket_id]]
        return []

    def upload(self, bucket_id, name, content, **kwargs):
        self.objects[bucket_id][name] = content
        return {'name': name}

    def get_url(self, bucket_id, name, **kwargs):
        return f"https://osb.org/{bucket_id}/{name}"

    def download(self, url, block_size=65536, headers=None, **kwargs):
        pass

    def delete_object(self, bucket_id, name, **kwargs):
        if bucket_id in self.objects:
            if name in self.objects[bucket_id]:
                del self.objects[bucket_id][name]

    def copy_object(self, bucket_id, name, dst_bucket_id, dst_name, **kwargs):
        self.objects[dst_bucket_id][dst_name] = self.objects[bucket_id][name]
