import os
import logging
import time
import math
import statistics

import yaml
from faker import Faker
import randomio

from os_benchmark import errors
from os_benchmark.drivers import utils as driver_utils

logger = logging.getLogger('osb.utils')
faker = Faker()


def get_config_file(config_file=None):
    """
    Get full configuration
    """
    if 'OSB_CONFIG_FILE' in os.environ:
        files = [os.environ['OSB_CONFIG_FILE']]
    elif config_file is not None:
        files = [config_file]
    else:
        files = ['~/.osb.yml', '/etc/osb.yml']

    configs = None
    for filename in files:
        filename = os.path.expanduser(filename)
        try:
            with open(filename) as fd:
                configs = yaml.full_load(fd)
                break
        except FileNotFoundError:
            continue

    logger.info("Use config file '%s'", filename)
    if not configs:
        msg = "No configuration file found."
        raise errors.ConfigurationError(msg)
    return configs


def get_driver_config(config_name=None, config_file=None):
    """
    Get driver configuration as dict
    """
    configs = get_config_file(config_file=config_file)

    if len(configs) == 1 and config_name is None:
        config_name = list(configs.keys())[0]
        logger.debug("Use the single driver config '%s'", config_name)

    if config_name and config_name not in configs:
        msg = "'%s' config found." % config_name
        raise errors.ConfigurationError(msg)

    if config_name is None:
        msg = 'Unknown configuration, please specify one in %s' % (
            ', '.join([c for c in configs])
        )
        raise errors.ConfigurationError(msg)

    return configs[config_name]


def get_driver(config):
    """Get configured driver"""
    key = config.pop('driver')
    driver_class = driver_utils.get_driver_class(key)
    logger.debug("Driver configured with '%s'", config)
    driver = driver_class(**config)
    return driver


def get_random_name(size=30, prefix=None):
    """Creates a random name"""
    name = faker.user_name()
    while len(name) < size:
        name += faker.user_name()
    if prefix:
        name = prefix + name
    return name[:size]


def get_random_content(size):
    """Creates a random fileobj"""
    return randomio.FileGenerator(size)


def timeit(func, *args, **kwargs):
    """Time a function"""
    start = time.time()
    output = func(*args, **kwargs)
    end = time.time()
    elapsed = end - start
    return elapsed, output


async def async_timeit(func, *args, **kwargs):
    """Time a function"""
    start = time.time()
    output = await func(*args, **kwargs)
    end = time.time()
    elapsed = end - start
    return elapsed, output


def percentile(values, percent):
    if not values:
        return
    values = sorted(values)
    return values[int(math.ceil((len(values) * percent) / 100)) - 1]

def percentile95(values):
    return percentile(values, 95)
