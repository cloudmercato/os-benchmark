import os
import tempfile
import time
from unittest import TestCase
import yaml
from os_benchmark import utils, errors


def create_config_file(config, filename=None):
    filename = filename or os.path.expanduser('~/.osb.yml')
    with open(filename, 'w') as fd:
        yaml.dump(config, stream=fd)


class GetConfigFileTest(TestCase):
    def test_environ(self):
        filename = tempfile.mktemp()
        create_config_file({'foo': {'driver': 'ram'}}, filename)
        os.environ['OSB_CONFIG_FILE'] = filename
        configs = utils.get_config_file()
        del os.environ['OSB_CONFIG_FILE']

    def test_specified(self):
        filename = tempfile.mktemp()
        create_config_file({'foo': {'driver': 'ram'}}, filename)
        configs = utils.get_config_file(filename)

    def test_default(self):
        filename = os.path.expanduser('~/.osb.yml')
        create_config_file({'foo': {'driver': 'ram'}}, filename)
        configs = utils.get_config_file()
        os.remove(filename)

    def test_bad_yaml(self):
        self.skipTest("Safe parsing enabled.")
        filename = tempfile.mktemp()
        with open(filename, 'w') as fd:
            fd.write('{1:2}')
        with self.assertRaises(errors.ConfigurationError):
            utils.get_config_file(filename)

    def test_no_file_found(self):
        with self.assertRaises(errors.ConfigurationError):
            configs = utils.get_config_file()


class GetDriverConfigTest(TestCase):
    def test_default_if_only_1_profile(self):
        create_config_file({'foo': {'driver': 'ram'}})
        driver_cfg = utils.get_driver_config()

    def test_profile_specified_but_not_found(self):
        filename = tempfile.mktemp()
        create_config_file({'foo': {'driver': 'ram'}}, filename)
        with self.assertRaises(errors.ConfigurationError):
            utils.get_driver_config(config_name='bar')
    
    def test_no_profile_found(self):
        create_config_file({})
        with self.assertRaises(errors.ConfigurationError):
            utils.get_driver_config(config_name='bar')


class GetDriverTest(TestCase):
    def test_func(self):
        config = {'driver': 'ram'}
        driver = utils.get_driver(config)

    def test_driver_not_found(self):
        config = {'driver': 'foo'}
        with self.assertRaises(Exception):
            driver = utils.get_driver(config)


class GetRandomNameTest(TestCase):
    def test_func(self):
        name = utils.get_random_name(20)
        self.assertEqual(len(name), 20)


class GetRandomContentTest(TestCase):
    def test_func(self):
        fd = utils.get_random_content(42)
        content = fd.read()
        self.assertEqual(len(content), 42)


class TimeItTest(TestCase):
    def test_output(self):
        def func():
            return True
        elapsed, output = utils.timeit(func)
        self.assertIsInstance(elapsed, float)
        self.assertIsInstance(output, bool)
        self.assertTrue(output)

    def test_elapsed(self):
        def func():
            time.sleep(1)
        elapsed, _ = utils.timeit(func)
        self.assertGreater(elapsed, 1)
        self.assertLess(elapsed, 1.1)
