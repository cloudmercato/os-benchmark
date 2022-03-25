"""Driver errors module."""
from os_benchmark import errors as base


class DriverError(base.OsbError):
    """Base Driver error"""


class DriverConnectionError(DriverError):
    """Driver connection error"""


class DriverAuthenticationError(DriverError):
    """Driver authentication error"""


class DriverPermissionError(DriverError):
    """Driver permission error"""


class DriverConfigError(DriverError):
    """Driver configuration error"""


class DriverNonEmptyBucketError(DriverError):
    """Driver non-empty bucket error"""


class DriverBucketUnfoundError(DriverError):
    """Driver bucket unfound error"""


class DriverBucketAlreadyExistError(DriverError):
    """Driver bucket already exist error"""


class DriverObjectUnfoundError(DriverError):
    """Driver object unfound error"""


class DriverStorageQuotaError(DriverError):
    """Driver service storage quota error"""


class DriverFeatureNotImplemented(DriverError):
    """The storage service doesn't implement a feature"""
