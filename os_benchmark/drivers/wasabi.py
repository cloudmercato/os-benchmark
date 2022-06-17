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
    region_name: <your_region>

Possible region IDs available at the following urls:

- https://s3.wasabisys.com/?describeRegions
- https://wasabi-support.zendesk.com/hc/en-us/articles/360015106031

.. _boto3: https://github.com/boto/boto3
"""
import xml.dom.minidom
from urllib.parse import urljoin

import urllib3.exceptions

from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """Wasabi S3 Driver"""
    id = 'wasabi'
    describe_regions_url = 'https://s3.wasabisys.com/?describeRegions'
    endpoint_url = 'https://s3.wasabisys.com'
    endpoint_urls = {}

    try:
        http = urllib3.PoolManager()
        r = http.request('GET', describe_regions_url)
        doc = xml.dom.minidom.parseString(r.data)
        for item in doc.getElementsByTagName('item'):
            region = item.getElementsByTagName('Region')[0].firstChild.data
            endpoint = item.getElementsByTagName('Endpoint')[0].firstChild.data
            endpoint_urls[region] = "https://%s" % endpoint
    except xml.parsers.expat.ExpatError as err:
        raise Exception("Unable parse Wasabi describeRegions XML payload.")
    except urllib3.exceptions.HTTPError as err:
        raise Exception("Unable connect to %s and verify Wasabi Regions." % describe_regions_url)

    default_kwargs = {
        'endpoint_url': endpoint_url,
    }

    def get_custom_kwargs(self, kwargs):
        if 'region_name' in kwargs:
            endpoint_url = self.endpoint_urls.get(kwargs['region_name'])
            kwargs['endpoint_url'] = endpoint_url
            self.endpoint_url = endpoint_url
        return kwargs

    def get_url(self, bucket_id, name, **kwargs):
        url = urljoin(self.endpoint_url, '%s/%s' % (bucket_id, name))
        return url
