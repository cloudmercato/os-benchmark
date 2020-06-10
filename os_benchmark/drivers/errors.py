"""Driver errors module."""
from os_benchmark import errors


class DriverError(errors.OsbError):
    """Base Driver error"""


class DriverConnecionError(DriverError):
    """Driver connection error"""


class DriverConfigError(DriverError):
    """Driver configuration error"""


class DriverNonEmptyBucketError(DriverError):
    """Driver non-empty bucket error"""
