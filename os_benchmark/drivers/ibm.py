"""
.. note::
  This driver requires `boto3`_ and `ibm-cos-sdk`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  ibm_s3:
    driver: ibm
    ibm_api_key_id: <your_api_key>
    ibm_service_instance_id: <your_cos_id>
    endpoint_url: <endpoint_url>

``ibm_api_key_id``: IBM API key
``ibm_service_instance_id``: COS ID, ie ``crn:v1:bluemix:public:cloud-object-storage:global:a/c067b1bc8f094716bcd8e3acca9d0230:c36aev98-23ad-4b1d-a63b-b2a7d7b7ecde::``

.. _boto3: https://github.com/boto/boto3
.. _ibm_boto3: https://github.com/IBM/ibm-cos-sdk-python
"""
from urllib.parse import urljoin
import ibm_boto3
import ibm_botocore
from os_benchmark.drivers import s3


class Driver(s3.Driver):
    """IBM Cloud Object Storage Driver"""
    id = 'ibm'

    @property
    def s3(self):
        if not hasattr(self, '_s3'):
            kwargs = {
                'ibm_auth_endpoint': "https://iam.cloud.ibm.com/identity/token",
            }
            kwargs.update(self.kwargs)
            config = {'signature_version': "oauth"}
            config.update(self.kwargs.get('config') or {})
            kwargs['config'] = ibm_botocore.client.Config(**config)
            self._s3 = ibm_boto3.resource("s3", **kwargs)
        return self._s3

    def get_url(self, bucket_id, name, **kwargs):
        url = urljoin(self.kwargs['endpoint_url'], '%s/%s' % (bucket_id, name))
        return url
