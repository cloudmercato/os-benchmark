Command line usage
==================

OS-Benchmark is powered by a command line tool: ``os-benchmark``

Benchmark
---------

Actually 2 scenarios are available ``time-upload`` and ``time-download``. They are designed to:

- Setup the test environment
- Launch tests
- Tear down
- Output result

Here's an exemple of output from ``os-benchmark``:

.. include:: _static/bench-output.txt
   :code:

time-upload
~~~~~~~~~~~

Example:::

  os-benchmark time-upload --object-size 1024 --object-number 1

time-download
~~~~~~~~~~~~~

Example:::

  os-benchmark time-download --object-size 1024 --object-number 1

Bucket management
-----------------

``os-benchmark`` provides operations for manage your bucket:

- ``create-bucket`` 
- ``list-buckets`` 
- ``delete-bucket`` 
- ``list-objects`` 
- ``upload`` 
- ``download`` 
- ``delete-object`` 
- ``clean-bucket``: Remove all files and delete a bucket
- ``clean``: Remove all objects and buckets
