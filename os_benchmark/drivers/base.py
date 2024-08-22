"""
Base Driver class module.
"""
from urllib.parse import urljoin
import logging

import tenacity
import concurrent.futures
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
RETRY = 3
RETRY_STATUS_CODE = (408, 413, 429, 500, 503, 504)
CONNECT_RETRY = 3
READ_RETRY = 1
STATUS_RETRY = 3

retry = tenacity.Retrying(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10)
)


class MultiPart:
    """Object simulating part from file-object for multipart-upload."""
    def __init__(self, file_object, size):
        self.file_object = file_object
        self.size = size
        self.offset = 0

    def read(self, chunksize=None):
        if self.offset >= self.size:
            return ''

        if (chunksize is None or chunksize < 0) or (chunksize + self.offset >= self.size):
            data = self.file_object.read(self.size - self.offset)
            self.offset = self.size
            return data

        self.offset += chunksize
        return self.file_object.read(chunksize)

    @property
    def len(self):
        return self.size

    def seek(self, pos, whence=0, /):
        self.offset = pos


class MultiPartUploader:
    """Helper creating a thread pool and splitting file in several parts."""
    def __init__(self, content, max_concurrency=None, multipart_chunksize=None, extra_upload_kwargs=None):
        self.content = content
        self.max_concurrency = max_concurrency or MAX_CONCURRENCY
        self.extra_upload_kwargs = extra_upload_kwargs or {}
        self.multipart_chunksize = multipart_chunksize or MULTIPART_CHUNKSIZE
        self.logger = logging.getLogger('osb.uploader')
        self.futures = []

    def run(self, upload_func):
        content_length = self.content.size
        part_id = 1
        offset = 0

        pool_kwargs = {'max_workers': self.max_concurrency}
        with concurrent.futures.ThreadPoolExecutor(**pool_kwargs) as executor:
            self.logger.debug('Started uploader')
            while offset < content_length:
                chunk_size = min(self.multipart_chunksize, content_length - offset)
                result = executor.submit(
                    upload_func,
                    content=self.content,
                    part_id=part_id,
                    offset=offset,
                    **self.extra_upload_kwargs,
                )
                self.logger.debug('Submitted part %s (%s)', part_id, offset)

                self.futures.append(result)
                part_id += 1
                offset += chunk_size

            self.logger.debug('Waiting all upload')
            results = [
                future.result()
                for future in self.futures
            ]
        return results


class BaseDriver:
    """Base Driver class"""
    id = None
    retry = RETRY
    read_timeout = READ_TIMEOUT
    connect_timeout = CONNECT_TIMEOUT
    retry_status_codes = RETRY_STATUS_CODE
    read_retry = READ_RETRY
    connect_retry = CONNECT_RETRY
    status_retry = STATUS_RETRY

    def __init__(
        self,
        retry=None,
        read_timeout=None,
        connect_timeout=None,
        read_retry=None,
        connect_retry=None,
        status_retry=None,
        **kwargs
    ):
        self.retry = retry or self.retry
        self.read_timeout = read_timeout or self.read_timeout
        self.connect_timeout = connect_timeout or self.connect_timeout
        self.read_retry = read_retry or self.read_retry
        self.connect_retry = connect_retry or self.connect_retry
        self.status_retry = status_retry or self.status_retry
        self.kwargs = self._validate_kwargs(kwargs)
        self.logger = logging.getLogger('osb.driver')

    def set_backend_logger(self, level):
        """Set logging level for underlyng library"""

    def setup(self, **kwargs):
        """Initialiaze driver before benchmark"""

    def _validate_kwargs(self, kwargs):
        """Ensure kwargs passed to __init__ are correct."""
        return kwargs

    def urljoin(self, *args):
        """Helpers for joining endpoint URL and path"""
        return urljoin(*args)

    def list_buckets(self, **kwargs):
        """List all buckets"""
        raise NotImplementedError()

    def create_bucket(self, name, **kwargs):
        """Create a bucket"""
        raise NotImplementedError()

    def delete_bucket(self, bucket_id, **kwargs):
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

    def download(self, url, block_size=65536, headers=None, **kwargs):
        """Download object from URL"""
        raise NotImplementedError()

    def delete_object(self, bucket_id, name, **kwargs):
        """Delete object from a bucket"""
        raise NotImplementedError()

    def prepare_delete_objects(self, names, **kwargs):
        """Prepare the formating of request"""
        return names

    def delete_objects(self, bucket_id, names, **kwargs):
        """Delete multiple objects from a bucket"""
        for name in names:
            self.delete_object(bucket_id, name, **kwargs)

    def copy_object(self, bucket_id, name, dst_bucket_id, dst_name, **kwargs):
        """Copy object to another bucket"""
        raise NotImplementedError()

    def get_bucket(self, bucket_id, **kwargs):
        """Get a bucket properties"""
        buckets = self.list_buckets()
        for bucket in buckets:
            if bucket['id'] == bucket_id:
                return bucket
        msg = "Bucket %s not found" % bucket_id
        raise errors.DriverBucketUnfoundError(msg)

    def get_object(self, bucket_id, name, **kwargs):
        """Get an object properties"""
        objs = self.list_objects(bucket_id=bucket_id, **kwargs)
        for obj in objs:
            if obj == name:
                return obj
        msg = "Object %s/%s not found" % (bucket_id, name)
        raise errors.DriverObjectUnfoundError(msg)

    def test_object_exists(self, bucket_id, name, check_version=False,
                           **kwargs):
        """Check if object exists or not"""
        exists = False
        try:
            self.get_object(bucket_id=bucket_id, name=name)
            exists = True
        except errors.DriverObjectUnfoundError:
            pass
        if check_version:
            versions = self.list_object_versions(bucket_id=bucket_id, name=name)
            if versions:
                exists = True
        return exists

    def put_object_tags(self, bucket_id, name, tags, **kwargs):
        """Add a tag to an object"""
        msg = "Object tagging not implemented by driver."
        raise NotImplementedError()

    def list_object_tags(self, bucket_id, name, **kwargs):
        """List an object's tags"""
        raise NotImplementedError()

    def list_objects_versions(self, bucket_id, **kwargs):
        """List all objects' versions"""
        raise NotImplementedError()

    def list_object_versions(self, bucket_id, name, **kwargs):
        """List all object' versions"""
        all_versions = self.list_objects_versions(bucket_id=bucket_id, **kwargs)
        versions = []
        for version in all_versions:
            if version['name'] == name:
                versions.append(version)
        return versions

    def remove_object_tags(self, bucket_id, name, tags, **kwargs):
        """Remove a tag to an object"""
        raise NotImplementedError()

    def put_object_lock(self, bucket_id, name, **kwargs):
        """Add a lock to an object"""
        raise NotImplementedError()

    def remove_object_lock(self, bucket_id, name, **kwargs):
        """Remove a lock to an object"""
        raise NotImplementedError()

    def get_object_torrent(self, bucket_id, name, **kwargs):
        """Get an object's torrent"""
        raise NotImplementedError()

    def list_multipart_uploads(self, bucket_id, **kwargs):
        """List multipart uploads in a bucket"""
        raise NotImplementedError()

    def list_delete_markers(self, bucket_id, **kwargs):
        """List delete markers in a bucket"""
        raise NotImplementedError()

    def delete_multipart_upload(self, bucket_id, name, upload_id, **kwargs):
        """Abort a multipart upload"""
        raise NotImplementedError()

    def put_bucket_cors(self, bucket_id, **kwargs):
        """Configure bucket's CORS permissions"""
        raise NotImplementedError()

    def put_bucket_tags(self, bucket_id, tags, **kwargs):
        """Attach tags to a bucket"""
        raise NotImplementedError()

    def list_bucket_tags(self, bucket_id, **kwargs):
        """List tags attached to a bucket"""
        raise NotImplementedError()

    def clean_bucket(self, bucket_id, delete_bucket=True, skip_lock=None):
        """
        Delete all object, version, multipart and delete markers from a bucket.
        By default, it removes also the bucket itself.
        """
        self.clean_bucket_objects(bucket_id=bucket_id, skip_lock=skip_lock)
        self.clean_bucket_versions(bucket_id=bucket_id)
        self.clean_bucket_delete_markers(bucket_id=bucket_id)
        self.clean_bucket_multiparts(bucket_id=bucket_id)
        if delete_bucket:
            retry.__call__(
                self.delete_bucket,
                bucket_id=bucket_id,
                skip_lock=skip_lock,
            )

    def clean_bucket_objects(self, bucket_id, skip_lock=True):
        try:
            self.logger.debug("Listing all objects from %s", bucket_id)
            objects = self.list_objects(bucket_id=bucket_id)
        except errors.DriverBucketUnfoundError as err:
            self.logger.debug(err)
            return
        # Do batch
        if len(objects) > 1:
            try:
                retry.__call__(
                    self.delete_objects,
                    bucket_id=bucket_id,
                    names=objects,
                    skip_lock=skip_lock,
                )
                return
            except NotImplementedError:
                pass
        # Or one-by-one
        for obj in objects:
            self.logger.info("Deleting object %s/%s", bucket_id, obj)
            retry.__call__(
                self.delete_object,
                bucket_id=bucket_id,
                name=obj,
                skip_lock=skip_lock
            )

    def clean_bucket_multiparts(self, bucket_id):
        try:
            parts = self.list_multipart_uploads(bucket_id)
        except NotImplementedError:
            return
        for part in parts:
            self.delete_multipart_upload(
                bucket_id=bucket_id,
                name=part['name'],
                upload_id=part['id'],
            )

    def clean_bucket_versions(self, bucket_id):
        try:
            versions = self.list_objects_versions(bucket_id=bucket_id)
        except (NotImplementedError, errors.DriverFeatureUnsupported):
            versions = []
        for version in versions:
            self.logger.info("Deleting object version %s/%s:%s", bucket_id, version['name'], version['id'])
            self.delete_object(
                bucket_id=bucket_id,
                name=version['name'],
                version_id=version['id'],
                skip_lock=True
            )

    def clean_bucket_delete_markers(self, bucket_id):
        try:
            markers = self.list_delete_markers(bucket_id=bucket_id)
        except (NotImplementedError, errors.DriverFeatureUnsupported):
            markers = []
        for marker in markers:
            self.logger.info("Deleting delete marker %s/%s:%s", bucket_id, marker['name'], marker['id'])
            self.delete_object(
                bucket_id=bucket_id,
                name=marker['name'],
                version_id=marker['id'],
                skip_lock=True
            )

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
            retry = Retry(
                total=self.retry,
                connect=self.connect_retry,
                read=self.read_retry,
                status=self.status_retry,
                status_forcelist=self.retry_status_codes,
                backoff_factor=0,
                redirect=0
            )
            timeout = (self.connect_timeout, self.read_timeout)
            adapter = HTTPAdapter(max_retries=retry, timeout=timeout)
            self._session.mount('http://', adapter)
            self._session.mount('https://', adapter)
        return self._session

    def download(self, url, block_size=65536, headers=None, **kwargs):
        self.logger.debug('GET %s', url)
        try:
            with self.session.get(url, stream=True, headers=headers) as response:
                if response.status_code != 200:
                    self.logger.warning('GET %s: %s', url, response.status_code)
                    msg = '%s %s' % (url, response.content)
                    raise errors.InvalidHttpCode(msg, response.status_code)
                for chunk in response.iter_content(chunk_size=block_size):
                    pass
        except requests.exceptions.ConnectionError as err:
            raise errors.DriverConnectionError(err.args[0])
