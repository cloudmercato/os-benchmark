Object Storage Benchmark
========================

.. image:: https://badge.fury.io/py/os-benchmark.svg
   :target: https://badge.fury.io/py/os-benchmark

.. image:: https://travis-ci.org/cloudmercato/os-benchmark.svg?branch=master
   :target: https://travis-ci.org/cloudmercato/os-benchmark

.. image:: https://coveralls.io/repos/github/cloudmercato/os-benchmark/badge.svg?branch=master
   :target: https://coveralls.io/github/cloudmercato/os-benchmark?branch=master

.. image:: https://readthedocs.org/projects/object-storage-benchmark/badge/?version=latest
   :target: https://object-storage-benchmark.readthedocs.io/?badge=latest
   :alt: Documentation Status

**OS-Benchmark** is a simple tool to measure object storage operations.

Features
--------

Benchmarks
~~~~~~~~~~

- **Upload Timing**
- **Download Timing**
- **Multi-part Download Timing**
- **Apache Benchmark**

Install
-------

::

  pip install os-benchmark
  
  
Usage
-----

Configuration
~~~~~~~~~~~~~

You need to specify a YAML config file such as following: ::

  ---
  my_exoscale:                                  # Configuration ID
      driver: exoscale                          # Driver key
      aws_access_key_id: mykey                  # Drivers params
      aws_secret_access_key: mysecrect
      endpoint_url: https://sos-ch-dk-2.exo.io
  
In command line, ``--config-file`` and ``--config-raw`` can be used to make the
choice, else ``~/.osb.yml``, then ``/etc/osb.yml`` will be used.

Command line
~~~~~~~~~~~~

::

  os-benchmark --help
  usage: os-benchmark [--config-file CONFIG_FILE] [--config-raw CONFIG_RAW]
                    [--config-name CONFIG_NAME] [-v {0,1,2}] [-i]
                    {help,create-bucket,list-buckets,delete-bucket,list-objects,upload,delete-object,clean-bucket,clean,time-upload,time-download,time-multi-download}
                    ...

  positional arguments:
    {help,create-bucket,list-buckets,delete-bucket,list-objects,upload,delete-object,clean-bucket,clean,time-upload,time-download}
                        Sub-command

  optional arguments:
    --config-file CONFIG_FILE
                        Specify a configuration file to use.
    --config-raw CONFIG_RAW
                        Provide a raw configuration as JSON instead of a
                        stored file.
    --config-name CONFIG_NAME
                        Select a driver configuration to use.
    -v {0,1,2}, --verbosity {0,1,2}
                        Verbosity level; 0=minimal output, 1=normal output
                        2=verbose output
    -i, --noinput       Disable any prompt
    
Test example: File uploading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

  $ os-benchmark time-upload --object-size 1 --object-number 5
  version         0.1
  operation       upload
  ops             5
  time            2.701468
  rate            1.850845
  bw              0.000002
  object_size     1
  total_size      5
  avg             0.539478
  stddev          0.126987
  med             0.537267
  min             0.415063
  max             0.744637


Contribute
----------

This project is created with ❤️ for free by `Cloud Mercato`_ under BSD License. Feel free to contribute by submitting a pull request or an issue.

.. _`Cloud Mercato`: https://www.cloud-mercato.com/
