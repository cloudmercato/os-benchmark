Object Storage Benchmark
========================

**OS-Benchmark** is a simple tool to measure object storage operations.

Features
--------

Benchmarks
~~~~~~~~~~

- **Upload Timing**
- **Download Timing**

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
  
In command line, `--config-file` and `--config-raw` can be used to make the
choice, else `~/.osb.yml`, then `/etc/osb.yml` will be used.

Command line
~~~~~~~~~~~~

::
  os-benchmark --help
