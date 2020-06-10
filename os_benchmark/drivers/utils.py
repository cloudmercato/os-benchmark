import importlib


def get_driver_class(key):
    """Get driver from its key"""
    module_path = 'os_benchmark.drivers.%s' % key
    module = importlib.import_module(module_path)
    driver_class = getattr(module, 'Driver')
    return driver_class
