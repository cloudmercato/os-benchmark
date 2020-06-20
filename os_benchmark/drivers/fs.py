"""
Filesystem driver storing buckets in file-system with true directories.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  myFsrofile:
    driver: fs
    path: /tmp/osn/
"""
import os
import shutil
from os_benchmark.drivers import base


class Driver(base.BaseDriver):
    def setup(self):
        os.makedirs(self.path)

    @property
    def path(self):
        if not hasattr(self, '_path'):
            self._path = self.kwargs['path']
        return self._path

    def _create_directory(self, path):
        os.makedirs(path)

    def _list_files(self, path):
        return os.listdir(path)

    def list_buckets(self, **kwargs):
        return [
            {'id': f} for f in self._list_files(self.path)
        ]

    def create_bucket(self, name, **kwargs):
        path = os.path.join(self.path, name)
        self._create_directory(path)
        return {'id': name}

    def delete_bucket(self, bucket_id):
        path = os.path.join(self.path, bucket_id)
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            self.logger.debug("Directory '%s' doesn't exist", path)

    def list_objects(self, bucket_id, **kwargs):
        path = os.path.join(self.path, bucket_id)
        try:
            self._create_directory(path)
        except FileExistsError:
            pass
        bucket_files = []
        for root, dirs, files in os.walk(path):
            for file_ in files:
                bucket_files.append(
                    file_
                )
        return bucket_files

    def upload(self, bucket_id, name, content, **kwargs):
        path = os.path.join(self.path, bucket_id, name)
        directory_path = os.path.dirname(path)
        try:
            self._create_directory(directory_path)
        except FileExistsError:
            pass

        with open(path, 'wb') as fd:
            shutil.copyfileobj(
                fsrc=content,
                fdst=fd,
            )
        return {'name': name}

    def get_url(self, bucket_id, name, **kwargs):
        path = os.path.join(self.path, bucket_id, name)
        url = 'file://%s' % path
        return url

    def download(self, url, block_size=2048, **kwargs):
        path = url.replace('file://', '')
        with open(path, 'rb') as fd:
            while fd.read(block_size):
                pass

    def delete_object(self, bucket_id, name, **kwargs):
        bucket_path = os.path.join(self.path, bucket_id)
        obj_path = os.path.join(bucket_id, name)
        abs_obj_path = os.path.join(self.path, obj_path)
        try:
            os.remove(abs_obj_path)
        except FileNotFoundError:
            self.logger.debug("File '%s' doesn't exist", abs_obj_path)
        except NotADirectoryError:
            self.logger.debug("'%s' is a directory", abs_obj_path)
        # Delete dirs if empty
        dirs = [i for i in obj_path.split('/') if i]
        bucket_i = dirs.index(bucket_id)+1
        for i in range(len(dirs))[::-1]:
            if i == bucket_i:
                break
            dir_path = '/'.join(dirs[:i])
            abs_dir_path = os.path.join(self.path, dir_path)
            try:
                files = self._list_files(abs_dir_path)
            except FileNotFoundError:
                self.logger.debug("File '%s' doesn't exist", obj_path)
            else:
                if not files:
                    break
            shutil.rmtree(abs_dir_path)
