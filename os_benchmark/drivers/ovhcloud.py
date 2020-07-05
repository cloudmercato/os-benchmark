"""
.. note::
  This driver requires `python-swiftclient`_.

`Object Storage`_ from `OVHcloud`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  ovhcloud:
    driver: ovhcloud
    user: <your_tenant_id>:<your_username>
    key: <your_password>
    tenant_name: "<your tenant_name>"
    os_options: 
      tenant_id: <your_tenant_id>
      region_name: <region_id>
      project_name: <your_project_name>
      project_id: <you_project_id>

.. warning::
    `tenant_name` **must** be surrounded by quotes.

.. _python-swiftclient: https://github.com/openstack/python-swiftclient
.. _`Object Storage`: https://www.ovhcloud.com/en/public-cloud/object-storage/
.. _OVHcloud: https://www.ovhcloud.com/
"""

from os_benchmark.drivers import swift


class Driver(swift.Driver):
    """OVHcloud Swift Driver"""
    id = 'ovhcloud'
    default_kwargs = {
        'authurl': 'https://auth.cloud.ovh.net/v3/',
        'auth_version': 3,
    }

    def get_url(self, bucket_id, name, **kwargs):
        url = '%s/%s/%s' % (self.swift.url, bucket_id, name)
        return url
