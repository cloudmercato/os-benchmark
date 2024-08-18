import importlib


def get_driver_class(key):
    """Get driver from its key"""
    if '.' in key:
        *module_path, class_name = key.split('.')
        module_path = '.'.join(module_path)
    else:
        module_path = 'os_benchmark.drivers.%s' % key
        class_name = 'Driver'
    module = importlib.import_module(module_path)
    driver_class = getattr(module, class_name)
    return driver_class
