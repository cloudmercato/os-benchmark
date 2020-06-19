"""Error module"""
class OsbError(Exception):
    """Base OS Benchmark error"""


class ConfigurationError(OsbError):
    """Error with configuration file"""


class InvalidHttpCode(OsbError):
    """Invalid HTTP response from server"""
