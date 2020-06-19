"""Driver errors module."""
from os_benchmark import errors as base


class DriverError(base.OsbError):
    """Base Driver error"""


class DriverConnecionError(DriverError):
    """Driver connection error"""


class DriverConfigError(DriverError):
    """Driver configuration error"""


class DriverNonEmptyBucketError(DriverError):
    """Driver non-empty bucket error"""


class DriverBucketUnfoundError(DriverError):
    """Driver bucket unfound error"""


class DriverObjectUnfoundError(DriverError):
    """Driver object unfound error"""
