"""
.. note::
  This driver requires `python-swiftclient`_.

Base `Openstack Swift`_ driver allowing usage of any Swift-based storage.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  swift_connection:
    driver: swift
    user: <your_username>
    key: <your_password>
    authurl: <auth_url>

All parameters except ``driver`` will be passed to ``swiftclient.Connection``.

.. _python-swiftclient: https://github.com/openstack/python-swiftclient
.. _`Openstack Swift`: https://github.com/openstack/swift
"""
import swiftclient
from swiftclient.service import SwiftService, SwiftUploadObject
from keystoneauth1 import exceptions as keystone_exceptions
from os_benchmark.drivers import base, errors


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 'swift'
    default_kwargs = {}

    @property
    def swift(self):
        if not hasattr(self, '_swift'):
            kwargs = self.default_kwargs.copy()
            os_options = kwargs.pop('os_options', {})
            kwargs.update(self.kwargs)
            kwargs['os_options'].update(os_options)
            self._swift = swiftclient.Connection(**kwargs)
        return self._swift

    def setup(self, **kwargs):
        self.service_kwargs = kwargs.copy()
        self.service_kwargs.update(
            retries=0
        )

        if 'max_concurrency' in kwargs:
            self.service_kwargs.update(
                object_threads=kwargs['max_concurrency'],
                object_uu_threads=kwargs['max_concurrency'],
                segment_threads=kwargs['max_concurrency'],
            )
        if 'multipart_chunksize' in kwargs:
            self.service_kwargs['segment_size'] = kwargs['multipart_chunksize']
        swiftclient.service._default_global_options.update(self.service_kwargs)
        self.logger.debug("Swift default config: %s", self.service_kwargs)
        self.swift  # Avoid lazyness

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
        try:
            self.swift.put_container(name, headers=headers)
        except keystone_exceptions.BadRequest as err:
            if err.http_status == 400:
                if 'application credential' in err.message:
                    raise errors.DriverAuthenticationError(err.message)
            raise
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

    def delete_object(self, bucket_id, name, **kwargs):
        try:
            self.swift.delete_object(bucket_id, name)
        except swiftclient.ClientException as err:
            if err.http_status == 404:
                return
            raise

    def upload(self, bucket_id, name, content, acl='public-read', **kwargs):
        self.swift.put_object(bucket_id, name, content)
        return {'name': name}
