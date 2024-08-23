"""Driver errors module."""
from os_benchmark import errors as base
from os_benchmark.errors import InvalidHttpCode


class DriverError(base.OsbError):
    """Base Driver error"""


class DriverConnectionError(DriverError):
    """Driver connection error"""


class DriverConnectionTimeoutError(DriverConnectionError):
    """Driver connection time out error"""


class DriverReadTimeoutError(DriverConnectionError):
    """Driver read time out error"""


class DriverAuthenticationError(DriverError):
    """Driver authentication error"""


class DriverServerError(DriverError):
    """Driver internal server error"""


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


class DriverFeatureUnsupported(DriverError):
    """The storage service doesn't implement a feature"""


class DriverRateLimitError(DriverError):
    """The storage service indicates the user has sent too many requests"""


class DriverObjectAclError(DriverError):
    """The storage service indicates that there's a problem with object ACLs."""


class DriverClientError(DriverError):
    """The local client is unable to run."""


class DriverClientCapacityError(DriverClientError):
    """The local client do not have enough capacity to run."""
