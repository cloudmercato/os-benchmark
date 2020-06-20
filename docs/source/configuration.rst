Configuration
=============

As Object storages require authentication and other custom parameters,
this software provide a way to use several configuration.

File format
-----------

Drivers configuration are stored as a YAML file containing one or several profile. Blow an example of file:

.. code-block:: yaml

    ---
    exoscale-dk-2:
      driver: exoscale
      aws_access_key_id: MyKey
      aws_secret_access_key: MySecret
      endpoint_url: https://sos-ch-dk-2.exo.io

    exoscale-vie-1:
      driver: exoscale
      aws_access_key_id: MyKey
      aws_secret_access_key: MySecret
      endpoint_url: https://sos-ch-vie-1.exo.io

This file provides two profiles for an Exoscale benchmark, on in DK-2 and the other one in CH-VIE-1.

File choice
-----------

The general behavior is the following:

#. Configuration specified by user via CLI or Python
#. If environment variable ``OSB_CONFIG_FILE`` is set
#. ``~/.osb.yml``
#. ``/etc/osb.yml``

Only the first file is used.
