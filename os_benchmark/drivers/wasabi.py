"""
.. note::
  This driver requires `boto3`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  wasabi:
    driver: wasabi
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    region_name: <region_name>

Possible region IDs available at the following urls:

- https://s3.wasabisys.com/?describeRegions
- https://wasabi-support.zendesk.com/hc/en-us/articles/360015106031

.. _boto3: https://github.com/boto/boto3
"""
from xml.dom import minidom
from urllib.parse import urljoin
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Wasabi S3 Driver"""
    id = 'wasabi'
    endpoint_url = 'https://s3.wasabisys.com'
    default_kwargs = {
        'endpoint_url': endpoint_url,
    }
    old_acl = False

    @property
    def endpoint_urls(self):
        if not hasattr(self, '_endpoint_urls'):
            self._endpoint_urls = {}
            params = {'describeRegions': ''}
            data = self.session.get(self.endpoint_url, params=params).text
            doc = minidom.parseString(data)
            for item in doc.getElementsByTagName('item'):
                region = item.getElementsByTagName('Region')[0].firstChild.data
                endpoint = item.getElementsByTagName('Endpoint')[0].firstChild.data
                self.endpoint_urls[region] = "https://%s" % endpoint
        return self._endpoint_urls

    def get_custom_kwargs(self, kwargs):
        if 'region_name' in kwargs:
            endpoint_url = self.endpoint_urls.get(kwargs['region_name'])
            kwargs['endpoint_url'] = endpoint_url
            self.endpoint_url = endpoint_url
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        endpoint_url = self.get_endpoint_url()
        url = urljoin(endpoint_url, '%s/%s' % (bucket_id, name))
        return url
