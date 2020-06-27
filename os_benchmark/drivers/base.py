"""
Base Driver class module.
"""
import logging
import requests
from os_benchmark.drivers import errors


class BaseDriver:
    """Base Driver class"""
    def __init__(self, **kwargs):
        self.kwargs = self._validate_kwargs(kwargs)
        self.logger = logging.getLogger('osb.driver')

    def setup(self):
        """Initialiaze driver before benchmark"""

    def _validate_kwargs(self, kwargs):
        """Ensure kwargs passed to __init__ are correct."""
        return kwargs

    def list_buckets(self, **kwargs):
        """List all buckets"""
        raise NotImplementedError()

    def create_bucket(self, name, **kwargs):
        """Create a bucket"""
        raise NotImplementedError()

    def delete_bucket(self, bucket_id):
        """Delete a bucket"""
        raise NotImplementedError()

    def list_objects(self, bucket_id, **kwargs):
        """List objects from a bucket"""
        raise NotImplementedError()

    def upload(self, bucket_id, name, content, **kwargs):
        """Upload an object into a bucket"""
        raise NotImplementedError()

    def get_url(self, bucket_id, name, **kwargs):
        """Get object URL"""
        raise NotImplementedError()

    def download(self, url, block_size=65536, **kwargs):
        """Download object from URL"""
        raise NotImplementedError()

    def delete_object(self, bucket_id, name, **kwargs):
        """Delete object from a bucket"""
        raise NotImplementedError()

    def clean_bucket(self, bucket_id, delete_bucket=True):
        """Delete all object from a bucket"""
        try:
            objects = self.list_objects(bucket_id=bucket_id)
        except errors.DriverBucketUnfoundError as err:
            self.logger.debug(err)
            return
        for obj in objects:
            self.delete_object(bucket_id=bucket_id, name=obj)
        if delete_bucket:
            self.delete_bucket(bucket_id=bucket_id)

    def clean(self):
        """Delete all buckets and all object"""
        for bucket in self.list_buckets():
            self.clean_bucket(bucket_id=bucket['id'])
            self.delete_bucket(bucket_id=bucket['id'])


class RequestsMixin:
    """Mixin providing a HTTTP Session"""
    session_headers = {}

    @property
    def session(self):
        if not hasattr(self, '_session'):
            self._session = requests.Session()
            self._session.headers = self.session_headers.copy()
        return self._session

    def download(self, url, block_size=65536, **kwargs):
        self.logger.debug('GET %s', url)
        with self.session.get(url, stream=True) as response:
            if response.status_code != 200:
                msg = '%s %s' % (url, response.content)
                raise errors.base.InvalidHttpCode(msg)
            for chunk in response.iter_content(chunk_size=block_size):
                pass
