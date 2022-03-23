"""
.. note::
  This driver requires `minio`_ and mc go client.

Base S3 driver using Minio SDK & mc go client allowing usage of any S3-based storage.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  minio:
    driver: minio_cli
    aws_access_key_id: <your_ak>
    aws_secret_access_key: <your_sk>
    region: eu-west-1

All parameters except ``driver`` will be passed to ``minio.Minio``.
"""
import subprocess
from shutil import copyfileobj
import threading

from os_benchmark.drivers import base, errors
from os_benchmark.drivers.minio_sdk import Driver as BaseDriver


class Driver(BaseDriver):
    id = 'minio_cli'

    def set_backend_logger(self, verbosity):
        self.verbosity = verbosity

    def upload(self, bucket_id, name, content, acl=None,
               multipart_threshold=None, multipart_chunksize=None,
               max_concurrency=None,
               **kwargs):
        acl = acl or self.default_object_acl
        target = '%s/%s/%s' % (self.id, bucket_id, name)
        metadata = {}
        cmd = ['mc', 'pipe', target]
        if self.verbosity == 4:
            cmd += ['--debug']
        if acl is not None:
            metadata['x-amz-acl'] = acl
        if metadata:
            cmd += ['--attr', ';'.join(['='.join(i) for i in metadata.items()])]

        def writer():
            copyfileobj(content, process.stdin)
            process.stdin.close()

        self.logger.debug('Upload cmd: %s', ' '.join(cmd))
        with subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as process:
            thread = threading.Thread(target=writer)
            thread.start()
            thread.join()
            rc = process.wait()
            if rc:
                raise errors.DriverError(process.stderr.read().decode())

            if self.verbosity >= 4:
                for line in process.stdout.readlines():
                    self.logger.debug(line.decode().strip())
            if self.verbosity >= 3:
                for line in process.stderr.readlines():
                    self.logger.debug(line.decode().strip())
        return {'name': name}
