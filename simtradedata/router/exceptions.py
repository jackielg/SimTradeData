"""Custom exceptions for the data source router."""


class DataSourceError(Exception):
    """All sources exhausted for a data request."""


class NoSourceAvailable(DataSourceError):
    """No source configured for this data_type + market combination."""
