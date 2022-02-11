"""
.. note::
  This driver requires `uplink-python`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  storj:
    driver: storj
    satellite: <satellite_id>
    api_key: <key>
    passphrase: <pass>

.. _uplink-python: https://github.com/storj-thirdparty/uplink-python
"""
from urllib.parse import urlparse
from uplink_python.uplink import Uplink
from uplink_python import errors as uplink_errors
from uplink_python import module_classes
from os_benchmark.drivers import base, errors


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 'storj'

    @property
    def uplink(self):
        if not hasattr(self, '_uplink'):
            self._uplink = Uplink()
        return self._uplink

    @property
    def access(self):
        if not hasattr(self, '_access'):
            try:
                if self.kwargs.get('api_key'):
                    self._access = self.uplink.request_access_with_passphrase(
                        satellite=self.kwargs['satellite'],
                        api_key=self.kwargs['api_key'],
                        passphrase=self.kwargs['passphrase'],
                    )
                elif 'access_grant' in self.kwargs:
                    self._access = self.uplink.parse_access(self.kwargs['access_grant'])
            except uplink_errors.InternalError as err:
                msg = err.details.splitlines()[0]
                raise errors.DriverConfigError(msg)
        return self._access

    @property
    def project(self):
        if not hasattr(self, '_project'):
            self._project = self.access.open_project()
        return self._project

    def _get_shared_access(self, bucket_id):
        if not hasattr(self, '_bucket_accesses'):
            self._bucket_accesses = {}

        if bucket_id not in self._bucket_accesses:
            permissions = module_classes.Permission(allow_list=True, allow_delete=False)
            shared_prefix = [module_classes.SharePrefix(bucket=bucket_id, prefix="")]
            shared_access = self.access.share(permissions, shared_prefix)
            self._bucket_accesses[bucket_id] = shared_access
        return self._bucket_accesses[bucket_id]

    def _get_shared_project(self, bucket_id):
        if not hasattr(self, '_projects'):
            self._projects = {}
        if bucket_id not in self._projects:
            shared_access = self._get_shared_access(bucket_id)
            shared_project = shared_access.open_project()
            self._projects[bucket_id] = shared_project
        return self._projects[bucket_id]

    def list_buckets(self, **kwargs):
        buckets = self.project.list_buckets()
        return [{'id': c.id_} for c in buckets]

    def create_bucket(self, name, acl='public-read', **kwargs):
        bucket = self.project.create_bucket(name)
        return {
            'id': bucket.name,
            'name': bucket.name,
        }

    def delete_bucket(self, bucket_id, **kwargs):
        try:
            self.project.delete_bucket(bucket_id)
        except uplink_errors.BucketNotFoundError:
            return
        except uplink_errors.BucketNotEmptyError as err:
            raise errors.DriverNonEmptyBucketError(err.message)

    def list_objects(self, bucket_id, **kwargs):
        objs = self.project.list_objects(
            bucket_id,
            module_classes.ListObjectsOptions(
                recursive=True,
                system=True,
            )
        )
        return [o.key for o in objs]

    def upload(self, bucket_id, name, content, max_concurrency=None,
               multipart_chunksize=None, multipart_threshold=None,
               validate_content=False, **kwargs):
        upload = self.project.upload_object(
            bucket_id,
            name,
        )
        upload.write_file(content)
        upload.commit()
        return {'name': name}

    def delete_object(self, bucket_id, name, **kwargs):
        self.project.delete_object(
            bucket_id,
            name,
        )

    def download(self, url, block_size=65536, **kwargs):
        self.logger.debug('GET %s', url)
        parsed_url = urlparse(url)
        download = self.project.download_object(parsed_url.netloc, parsed_url.path[1:])
        block_num = (download.file_size() // block_size) + 1
        for i in range(block_num):
            download.read(block_size)

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        # region = self.kwargs['satellite'].split('@')[1].split('.')[0]
        # url = 'https://link.%s.storjshare.io/s/%s/%s/%s' % (
        #     region,
        #     self.kwargs['access_key'],
        #     bucket_id,
        #     name,
        # )
        url = 'storj://%s/%s' % (
            bucket_id, name
        )
        return url
