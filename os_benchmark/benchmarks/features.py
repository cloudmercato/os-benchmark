from os_benchmark import utils
from os_benchmark.drivers import errors as driver_errors
from . import base


class DeleteObjectsTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        base_obj_name = utils.get_random_name()
        self.objects = []
        # Setup
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )
        for i in range(3):
            obj_name = '%s-%s' % (base_obj_name, i)
            content = utils.get_random_content(1)
            obj = self.driver.upload(
                bucket_id=self.bucket['id'],
                storage_class=self.params['storage_class'],
                name=obj_name,
                content=content,
            )
            self.objects.append(obj)
        self.prep_request = self.driver.prepare_delete_objects(
            bucket_id=self.bucket['id'],
            names=self.objects,
        )

    def run(self):
        self.driver.delete_objects(
            bucket_id=self.bucket['id'],
            names=self.objects,
            request=self.prep_request,
        )
        for obj in self.objects:
            exists = self.driver.test_object_exists(self.bucket['id'], obj)
            if exists:
                msg = "Object deletion not effective"
                raise driver_errors.DriverFeatureNotImplemented(msg)


    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])


class CopyObjectTest(base.BaseBenchmark):
    def setup(self):
        src_bucket_name = utils.get_random_name()
        dst_bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.src_bucket = self.driver.create_bucket(
            name=src_bucket_name,
            storage_class=self.params['storage_class'],
        )
        self.dst_bucket = self.driver.create_bucket(
            name=dst_bucket_name,
            storage_class=self.params['storage_class'],
        )
        content = utils.get_random_content(1)
        obj = self.driver.upload(
            bucket_id=self.src_bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.driver.copy_object(
            bucket_id=self.src_bucket['id'],
            name=self.obj_name,
            dst_bucket_id=self.dst_bucket['id'],
            dst_name=self.obj_name,
        )
        exists = self.driver.test_object_exists(self.dst_bucket['id'], self.obj_name)
        if not exists:
            msg = "Object copy not effective"
            raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.src_bucket['id'])
        self.driver.clean_bucket(self.dst_bucket['id'])


class GetObjectTorrentTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )
        content = utils.get_random_content(1)
        self.driver.upload(
            bucket_id=self.bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        torrent = self.driver.get_object_torrent(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
        )
        self.logger.debug('Torrent magnet: %s', torrent)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])


class LockObjectTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
            bucket_lock=True,
        )
        content = utils.get_random_content(1)
        self.driver.upload(
            bucket_id=self.bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.logger.debug("Putting lock on object")
        self.driver.put_object_lock(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
        )
        try:
            self.logger.debug("Trying lock object deletion")
            self.driver.delete_object(self.bucket['id'], self.obj_name, skip_lock=False)
        except Exception as err:
            self.logger.debug(err)
        else:
            exists = self.driver.test_object_exists(
                bucket_id=self.bucket['id'],
                name=self.obj_name,
                check_version=True,
            )
            if not exists:
                msg = "Object lock not effective"
                raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'], skip_lock=True)


class PutObjectTagTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )
        content = utils.get_random_content(1)
        self.driver.upload(
            bucket_id=self.bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.driver.put_object_tags(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
            tags={'foo': 'bar'},
        )
        tags = self.driver.list_object_tags(
            bucket_id=self.bucket['id'],
            name=self.obj_name,
        )
        if 'foo' not in tags:
            msg = "Object tagging not effective"
            raise driver_errors.DriverFeatureNotImplemented(msg)
        if tags['foo'] != 'bar':
            msg = "Wrong object tagging"
            raise driver_errors.DriverFeatureNotImplemented(msg)

    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])


class CorsTest(base.BaseBenchmark):
    def setup(self):
        bucket_name = utils.get_random_name()
        self.obj_name = utils.get_random_name()
        self.bucket = self.driver.create_bucket(
            name=bucket_name,
            storage_class=self.params['storage_class'],
        )
        content = utils.get_random_content(1)
        self.driver.upload(
            bucket_id=self.bucket['id'],
            storage_class=self.params['storage_class'],
            name=self.obj_name,
            content=content,
        )

    def run(self):
        self.driver.put_bucket_cors(
            bucket_id=self.bucket['id'],
        )
        url = self.driver.get_url(bucket_id=self.bucket['id'], name=self.obj_name)
        response = self.driver.session.get(url, headers={'Origin': 'foo'})
        if 'Access-Control-Allow-Origin' not in response.headers:
            msg = "CORS headers not present"
            raise driver_errors.DriverFeatureNotImplemented(msg)


    def tear_down(self):
        self.driver.clean_bucket(self.bucket['id'])


class TestFeatureBenchmark(base.BaseBenchmark):
    """Test and report feature availability"""

    TESTS = {
        'delete_objects': DeleteObjectsTest,
        'copy_object': CopyObjectTest,
        'get_torrent': GetObjectTorrentTest,
        'put_object_tag': PutObjectTagTest,
        'lock_object': LockObjectTest,
        'cors': CorsTest,
    }

    def setup(self):
        self.params['storage_class'] = self.params.get('storage_class')
        self.tests = {}
        for test_name, test_class in self.TESTS.items():
            test = test_class(driver=self.driver)
            test.set_params(
                storage_class=self.params['storage_class'],
            )
            self.tests[test_name] = test

    def run(self, **kwargs):
        self.results = {}
        for test_name, test in self.tests.items():
            self.logger.info('Run %s', test_name)
            test.setup()
            try:
                test.run()
            except driver_errors.DriverFeatureNotImplemented as err:
                self.logger.info("DriverFeatureNotImplemented: %s", err)
                self.results[test_name] = 'nok'
            except NotImplementedError:
                self.results[test_name] = 'not_implemented'
            except Exception as err:
                self.logger.error(err)
                self.logger.exception(err)
                self.results[test_name] = 'error'
            else:
                self.results[test_name] = 'ok'
            finally:
                test.tear_down()

    def make_stats(self):
        return self.results
