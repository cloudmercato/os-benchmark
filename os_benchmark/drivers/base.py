"""
Base Driver class module.
"""
import logging
import requests
from requests.adapters import HTTPAdapter as BaseHTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from os_benchmark.drivers import errors

USER_AGENT = 'os-benchmark'
MULTIPART_THRESHOLD = 64*2**20
MULTIPART_CHUNKSIZE = 64*2**20
MAX_CONCURRENCY = 10
CONNECT_TIMEOUT = 3
READ_TIMEOUT = 1


class BaseDriver:
    """Base Driver class"""
    id = None
    read_timeout = READ_TIMEOUT
    connect_timeout = CONNECT_TIMEOUT

    def __init__(self, read_timeout=None, connect_timeout=None, **kwargs):
        self.read_timeout = read_timeout or self.read_timeout
        self.connect_timeout = connect_timeout or self.connect_timeout
        self.kwargs = self._validate_kwargs(kwargs)
        self.logger = logging.getLogger('osb.driver')

    def setup(self, **kwargs):
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
            self.logger.debug("Listing all objects from %s", bucket_id)
            objects = self.list_objects(bucket_id=bucket_id)
        except errors.DriverBucketUnfoundError as err:
            self.logger.debug(err)
            return
        for obj in objects:
            self.logger.info("Deleting object %s/%s", bucket_id, obj)
            self.delete_object(bucket_id=bucket_id, name=obj)
        if delete_bucket:
            self.delete_bucket(bucket_id=bucket_id)

    def clean(self):
        """Delete all buckets and all object"""
        self.logger.debug("Listing all buckets")
        for bucket in self.list_buckets():
            self.logger.info("Deleting bucket %s", bucket['id'])
            self.clean_bucket(bucket_id=bucket['id'], delete_bucket=True)


class HTTPAdapter(BaseHTTPAdapter):
    def __init__(self, timeout=None, *args, **kwargs):
        self.timeout = 3 if timeout is None else timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

class RequestsMixin:
    """Mixin providing a HTTTP Session"""
    session_headers = {}

    @property
    def session(self):
        if not hasattr(self, '_session'):
            self._session = requests.Session()
            self._session.headers = self.session_headers.copy()
            retry = Retry(total=0)
            timeout = (self.connect_timeout, self.read_timeout)
            adapter = HTTPAdapter(max_retries=retry, timeout=timeout)
            self._session.mount('http://', adapter)
            self._session.mount('https://', adapter)
        return self._session

    def download(self, url, block_size=65536, **kwargs):
        self.logger.debug('GET %s', url)
        try:
            with self.session.get(url, stream=True) as response:
                if response.status_code != 200:
                    msg = '%s %s' % (url, response.content)
                    raise errors.base.InvalidHttpCode(msg, response.status_code)
                for chunk in response.iter_content(chunk_size=block_size):
                    pass
        except requests.exceptions.ConnectionError as err:
            raise errors.DriverConnectionError(err.args[0])
