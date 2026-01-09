"""Exceptions for taskiq-sqlite."""


class SQLiteError(Exception):
    """Base exception for SQLite errors."""


class ResultIsMissingError(SQLiteError):
    """Raised when result is not found in the database."""


class ExpireTimeMustBeMoreThanZeroError(SQLiteError):
    """Raised when expire time is less than or equal to zero."""


class DuplicateExpireTimeSelectedError(SQLiteError):
    """Raised when both ex_time and px_time are specified."""
