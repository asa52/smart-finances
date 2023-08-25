"""Directory for source code for Smart Finances app."""

DEFAULT_DATESTR_FORMAT = "%Y-%m-%d"
DEFAULT_START_DATE = "2017-09-01"
DEFAULT_CURRENCY = "GBP"


class NoDataWarning(Warning):
    """Raise Warning when no data present in file or other data source being
    read."""
